%% node_agent_ctrd_fake: deterministic in-memory executor for tests.
%%
%% Mirrors the semantics of node_agent_ctrd_real (event delivery via `!`,
%% sticky-terminal buffer eviction, synchronous subscribe with replay)
%% so the task gen_server runs identical code paths in both worlds.
%%
%% Test surface:
%%   inject_exit(Id, Code)         — synthesize an exit event.
%%   inject_oom(Id, Code)          — synthesize an OOM event.
%%   inject_pull_error(Id, Reason) — make the *next* `create(Id)` fail.
%%   flush(Pid)                    — sync fence: sys:get_state(Pid).
%%   preload(Items)                — seed containers for adoption tests.
%%   set_list_fail_count(N)        — fail next N list/0 calls.
%%   drop_next_exit(Id)            — suppress the exit event that kill/3
%%                                   would otherwise emit for Id.
%%   calls()                       — observed [{Op, Args}] in call order.
%%   reset()                       — clear all state (for per-test setup).
-module(node_agent_ctrd_fake).
-behaviour(gen_server).
-behaviour(node_agent_ctrd).

-export([start_link/0]).
-export([create/1, start/1, kill/3, delete/1, list/0,
         subscribe/2, unsubscribe/2]).
-export([inject_exit/2, inject_oom/2, inject_started/2, inject_pull_error/2,
         flush/1, preload/1, set_list_fail_count/1, drop_next_exit/1,
         calls/0, reset/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SRV, ?MODULE).
-define(BUFFER_LIMIT, 16).

-record(cont, {status :: running | stopped | unknown,
               pid    :: pos_integer() | undefined,
               spec   :: mycelium_proto:task_spec()}).

start_link() ->
    gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

%% --- node_agent_ctrd callbacks -----------------------------------------

create(Spec)          -> gen_server:call(?SRV, {create, Spec}).
start(Id)             -> gen_server:call(?SRV, {start, Id}).
kill(Id, Sig, TmoMs)  -> gen_server:call(?SRV, {kill, Id, Sig, TmoMs}).
delete(Id)            -> gen_server:call(?SRV, {delete, Id}).
list()                -> gen_server:call(?SRV, list).
subscribe(Pid, Id)    -> gen_server:call(?SRV, {subscribe, Pid, Id}).
unsubscribe(Pid, Id)  -> gen_server:call(?SRV, {unsubscribe, Pid, Id}).

%% --- test surface ------------------------------------------------------

inject_exit(Id, Code)        -> gen_server:call(?SRV, {inject, Id, {exited, #{code => Code}}}).
inject_oom(Id, Code)         -> gen_server:call(?SRV, {inject, Id, {oom_killed, #{code => Code}}}).
inject_started(Id, Pid)      -> gen_server:call(?SRV, {inject, Id, {started, #{pid => Pid}}}).
inject_pull_error(Id, Reason)-> gen_server:call(?SRV, {inject_pull_error, Id, Reason}).

%% A synchronous round-trip to Pid: once it returns, all messages that
%% were in Pid's mailbox before the call have been handled.
flush(Pid) when is_pid(Pid) ->
    _ = sys:get_state(Pid),
    ok.

preload(Items)               -> gen_server:call(?SRV, {preload, Items}).
set_list_fail_count(N)       -> gen_server:call(?SRV, {set_list_fail_count, N}).
drop_next_exit(Id)           -> gen_server:call(?SRV, {drop_next_exit, Id}).
calls()                      -> gen_server:call(?SRV, calls).
reset()                      -> gen_server:call(?SRV, reset).

%% --- gen_server --------------------------------------------------------

init([]) ->
    {ok, apply_boot_seeds(fresh())}.

handle_call({create, Spec}, _From, S) ->
    Id = maps:get(id, Spec),
    S1 = log(create, [Spec], S),
    PullErrs = maps:get(pull_errors, S1),
    case maps:take(Id, PullErrs) of
        {Reason, PullErrs1} ->
            {reply, {error, Reason}, S1#{pull_errors := PullErrs1}};
        error ->
            Conts = maps:get(containers, S1),
            C = #cont{status = stopped, spec = Spec},
            {reply, ok, S1#{containers := Conts#{Id => C}}}
    end;

handle_call({start, Id}, _From, S) ->
    S1 = log(start, [Id], S),
    case maps:get(Id, maps:get(containers, S1), undefined) of
        undefined ->
            {reply, {error, not_found}, S1};
        C ->
            FakePid = fake_pid(),
            C1 = C#cont{status = running, pid = FakePid},
            S2 = update_cont(Id, C1, S1),
            S3 = deliver(Id, {started, #{pid => FakePid}}, S2),
            {reply, ok, S3}
    end;

handle_call({kill, Id, _Sig, _TmoMs}, _From, S) ->
    S1 = log(kill, [Id, _Sig, _TmoMs], S),
    Drop = maps:get(drop_exits, S1),
    case sets:is_element(Id, Drop) of
        true ->
            S2 = S1#{drop_exits := sets:del_element(Id, Drop)},
            {reply, ok, S2};
        false ->
            case maps:get(Id, maps:get(containers, S1), undefined) of
                undefined ->
                    {reply, ok, S1};
                C ->
                    C1 = C#cont{status = stopped, pid = undefined},
                    S2 = update_cont(Id, C1, S1),
                    S3 = deliver(Id, {exited, #{code => 0, signal => 'SIGTERM'}}, S2),
                    {reply, ok, S3}
            end
    end;

handle_call({delete, Id}, _From, S) ->
    S1 = log(delete, [Id], S),
    Conts = maps:get(containers, S1),
    {reply, ok, S1#{containers := maps:remove(Id, Conts)}};

handle_call(list, _From, #{list_fail_count := N} = S) when N > 0 ->
    S1 = log(list, [], S),
    {reply, {error, {transient, list_fail_count}}, S1#{list_fail_count := N - 1}};
handle_call(list, _From, S) ->
    S1 = log(list, [], S),
    Items = [list_item(Id, C) || {Id, C} <- maps:to_list(maps:get(containers, S1))],
    {reply, {ok, Items}, S1};

handle_call({subscribe, Pid, Id}, _From, S) ->
    Subs = maps:get(subscribers, S),
    PrevSubs = maps:get(Id, Subs, []),
    NewSubs = case lists:member(Pid, PrevSubs) of
                  true -> PrevSubs;
                  false -> [Pid | PrevSubs]
              end,
    S1 = S#{subscribers := Subs#{Id => NewSubs}},
    %% Replay buffered events to the new subscriber. Semantic equivalence
    %% with the real path: events are delivered as plain messages.
    Buf = lists:reverse(maps:get(Id, maps:get(buffers, S1), [])),
    lists:foreach(fun(Evt) -> Pid ! {ctrd_event, Id, Evt} end, Buf),
    {reply, ok, S1};

handle_call({unsubscribe, Pid, Id}, _From, S) ->
    Subs = maps:get(subscribers, S),
    NewSubs = case maps:get(Id, Subs, []) of
                  [] -> [];
                  L  -> L -- [Pid]
              end,
    {reply, ok, S#{subscribers := Subs#{Id => NewSubs}}};

handle_call({inject, Id, Event}, _From, S) ->
    {reply, ok, deliver(Id, Event, S)};

handle_call({inject_pull_error, Id, Reason}, _From, S) ->
    PullErrs = maps:get(pull_errors, S),
    {reply, ok, S#{pull_errors := PullErrs#{Id => Reason}}};

handle_call({preload, Items}, _From, S) ->
    Conts0 = maps:get(containers, S),
    Conts = lists:foldl(
              fun(#{id := Id, status := St, spec := Sp} = It, Acc) ->
                      Pid0 = maps:get(pid, It, undefined),
                      Acc#{Id => #cont{status = St, pid = Pid0, spec = Sp}}
              end, Conts0, Items),
    {reply, ok, S#{containers := Conts}};

handle_call({set_list_fail_count, N}, _From, S) ->
    {reply, ok, S#{list_fail_count := N}};

handle_call({drop_next_exit, Id}, _From, S) ->
    {reply, ok, S#{drop_exits := sets:add_element(Id, maps:get(drop_exits, S))}};

handle_call(calls, _From, S) ->
    {reply, lists:reverse(maps:get(calls, S)), S};

handle_call(reset, _From, _S) ->
    {reply, ok, fresh()}.

handle_cast(_, S) -> {noreply, S}.
handle_info(_, S) -> {noreply, S}.

%% --- helpers -----------------------------------------------------------

%% Test seeds applied at gen_server boot, read from persistent_term. Lets
%% the adopter suite pre-populate state before node_agent_sup starts
%% children in order.
apply_boot_seeds(S) ->
    PreloadKey = {?MODULE, preload_items},
    FailCountKey = {?MODULE, list_fail_count},
    S1 = case persistent_term:get(PreloadKey, undefined) of
             undefined -> S;
             Items ->
                 Conts0 = maps:get(containers, S),
                 Conts = lists:foldl(
                           fun(#{id := Id, status := St, spec := Sp} = It, Acc) ->
                                   Pid0 = maps:get(pid, It, undefined),
                                   Acc#{Id => #cont{status = St, pid = Pid0, spec = Sp}}
                           end, Conts0, Items),
                 S#{containers := Conts}
         end,
    S2 = case persistent_term:get(FailCountKey, undefined) of
             undefined -> S1;
             N -> S1#{list_fail_count := N}
         end,
    S2.

fresh() ->
    #{subscribers     => #{},
      buffers         => #{},
      containers      => #{},
      calls           => [],
      pull_errors     => #{},
      drop_exits      => sets:new(),
      list_fail_count => 0,
      next_pid        => 10_000}.

log(Op, Args, S) ->
    S#{calls := [{Op, Args} | maps:get(calls, S)]}.

update_cont(Id, C, S) ->
    Conts = maps:get(containers, S),
    S#{containers := Conts#{Id => C}}.

list_item(Id, #cont{status = St, pid = Pid, spec = Spec}) ->
    M0 = #{id => Id, status => St, spec => Spec},
    case Pid of
        undefined -> M0;
        _         -> M0#{pid => Pid}
    end.

fake_pid() ->
    erlang:unique_integer([positive, monotonic]) + 10_000.

%% Deliver an event: fan out to subscribers, buffer for absent ones,
%% and update container state if the event is terminal.
deliver(Id, Event, S) ->
    Subs = maps:get(Id, maps:get(subscribers, S), []),
    case Subs of
        [] ->
            push_buffer(Id, Event, S);
        _ ->
            lists:foreach(fun(P) -> P ! {ctrd_event, Id, Event} end, Subs),
            S
    end.

push_buffer(Id, Event, S) ->
    Buffers = maps:get(buffers, S),
    Buf = maps:get(Id, Buffers, []),
    Buf1 = [Event | Buf],
    Buf2 = case length(Buf1) > ?BUFFER_LIMIT of
               true  -> evict_sticky_terminal(Buf1);
               false -> Buf1
           end,
    S#{buffers := Buffers#{Id => Buf2}}.

%% Buf is newest-first. Drop the oldest non-terminal; if none exist,
%% drop the oldest terminal. Guarantees the latest terminal is retained
%% as long as at least one non-terminal is available to evict.
evict_sticky_terminal(Buf) ->
    Rev = lists:reverse(Buf),  % oldest-first
    case drop_first_nonterminal(Rev, []) of
        {ok, Rest} -> lists:reverse(Rest);
        none       -> lists:reverse(tl(Rev))  % drop oldest (all terminal)
    end.

drop_first_nonterminal([], _Acc) ->
    none;
drop_first_nonterminal([E | Rest], Acc) ->
    case is_terminal(E) of
        false -> {ok, lists:reverse(Acc, Rest)};
        true  -> drop_first_nonterminal(Rest, [E | Acc])
    end.

is_terminal({exited, _})            -> true;
is_terminal({oom_killed, _})        -> true;
is_terminal({failed_permanent, _})  -> true;
is_terminal(_)                      -> false.
