%% node_agent_ctrd_real: production node_agent_ctrd impl.
%%
%% Translates behaviour calls into helper-protocol frames via
%% node_agent_helper_port, maintains per-task subscriber table and
%% event buffer (with sticky-terminal eviction), and routes incoming
%% helper events to subscribers.
%%
%% Event delivery contract (same as ctrd_fake): plain `!` messages of
%% the shape `{ctrd_event, TaskId, task_event()}`, buffered per TaskId
%% when no subscriber is present. Sticky-terminal eviction matches the
%% fake: when overfull, drop the oldest non-terminal; if all buffered
%% are terminal, drop the oldest.
-module(node_agent_ctrd_real).
-behaviour(gen_server).
-behaviour(node_agent_ctrd).

-export([start_link/0]).
-export([create/1, start/1, kill/3, delete/1, list/0,
         subscribe/2, unsubscribe/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SRV, ?MODULE).
-define(BUFFER_LIMIT, 16).

start_link() ->
    gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

%% --- node_agent_ctrd callbacks ---------------------------------------

create(Spec) ->
    Req = create_request(Spec),
    simple_ack(node_agent_helper_port:send_op(Req)).

start(Id) ->
    simple_ack(node_agent_helper_port:send_op(
                 #{op => start, id => Id, namespace => ns()})).

kill(Id, Sig, TimeoutMs) ->
    simple_ack(node_agent_helper_port:send_op(
                 #{op => kill, id => Id, namespace => ns(),
                   signal => atom_to_binary(Sig, utf8),
                   timeout_ms => TimeoutMs},
                 TimeoutMs + 5_000)).

delete(Id) ->
    simple_ack(node_agent_helper_port:send_op(
                 #{op => delete, id => Id, namespace => ns()})).

list() ->
    case node_agent_helper_port:send_op(
           #{op => list, namespace => ns()}) of
        {ok, #{<<"items">> := Items}} ->
            {ok, [list_item(It) || It <- items_or_empty(Items)]};
        {error, Err} ->
            {error, Err}
    end.

subscribe(Pid, Id) ->
    gen_server:call(?SRV, {subscribe, Pid, Id}).

unsubscribe(Pid, Id) ->
    gen_server:call(?SRV, {unsubscribe, Pid, Id}).

%% --- gen_server ------------------------------------------------------

init([]) ->
    %% Register self() with the helper port so event frames are routed
    %% here. Cast is fine — helper_port init is already done by the
    %% supervisor before we start.
    gen_server:cast(node_agent_helper_port, {register_event_sink, self()}),
    {ok, #{subscribers => #{}, buffers => #{}}}.

handle_call({subscribe, Pid, Id}, _From, S) ->
    Subs = maps:get(subscribers, S),
    Prev = maps:get(Id, Subs, []),
    NewList = case lists:member(Pid, Prev) of
                  true -> Prev;
                  false -> [Pid | Prev]
              end,
    S1 = S#{subscribers := Subs#{Id => NewList}},
    Buffers = maps:get(buffers, S1),
    Buf = maps:get(Id, Buffers, []),
    lists:foreach(fun(Evt) -> Pid ! {ctrd_event, Id, Evt} end, lists:reverse(Buf)),
    {reply, ok, S1};
handle_call({unsubscribe, Pid, Id}, _From, S) ->
    Subs = maps:get(subscribers, S),
    New = case maps:get(Id, Subs, []) of
              [] -> [];
              L -> L -- [Pid]
          end,
    {reply, ok, S#{subscribers := Subs#{Id => New}}}.

handle_cast(_Msg, S) -> {noreply, S}.

handle_info({helper_event, #{<<"kind">> := Kind, <<"id">> := Id} = Msg}, S) ->
    Event = decode_event(Kind, Msg),
    {noreply, deliver(Id, Event, S)};
handle_info(_Msg, S) ->
    {noreply, S}.

%% --- encoding / decoding ---------------------------------------------

create_request(Spec) ->
    SpecLabel = spec_label(Spec),
    Base = #{op => create,
             namespace => ns(),
             id => maps:get(id, Spec),
             image => maps:get(image, Spec),
             cmd => maps:get(cmd, Spec),
             env => maps:get(env, Spec, []),
             cwd => maps:get(cwd, Spec, <<"/">>),
             labels => #{<<"mycelium.spec">> => SpecLabel}},
    Base1 = case maps:get(cpu_max, Spec, undefined) of
                undefined -> Base;
                {Q, P} -> Base#{cpu_max => [Q, P]}
            end,
    case maps:get(mem_max, Spec, undefined) of
        undefined -> Base1;
        Mem -> Base1#{mem_max => Mem}
    end.

%% base64 of JSON-encoded spec; round-tripped via container label.
spec_label(Spec) ->
    base64:encode(node_agent_json:encode(Spec)).

list_item(#{<<"id">> := Id} = It) ->
    Status = status_of(maps:get(<<"status">>, It, <<"unknown">>)),
    Spec = decode_spec_label(maps:get(<<"spec_label">>, It, <<>>)),
    M = #{id => Id, status => Status, spec => Spec},
    case maps:get(<<"pid">>, It, undefined) of
        undefined -> M;
        Pid -> M#{pid => Pid}
    end.

status_of(<<"running">>) -> running;
status_of(<<"stopped">>) -> stopped;
status_of(_)             -> unknown.

decode_spec_label(<<>>) -> #{};
decode_spec_label(B) when is_binary(B) ->
    try
        Json = base64:decode(B),
        case node_agent_json:decode(Json) of
            {ok, Map} when is_map(Map) -> rekey_atoms(Map);
            _ -> #{}
        end
    catch _:_ -> #{}
    end.

%% Re-atomize the well-known keys so downstream code gets the
%% task_spec() it expects.
rekey_atoms(M) ->
    Keys = [id, image, cmd, env, cwd, cpu_max, mem_max, restart, stop_timeout_ms],
    lists:foldl(fun(K, Acc) ->
                        KB = atom_to_binary(K, utf8),
                        case maps:take(KB, Acc) of
                            error -> Acc;
                            {V, Acc1} -> Acc1#{K => V}
                        end
                end, M, Keys).

decode_event(<<"task_exit">>, Msg) ->
    Code = maps:get(<<"exit_code">>, Msg, -1),
    case maps:get(<<"signal">>, Msg, <<>>) of
        <<>>   -> {exited, #{code => Code}};
        SigBin -> {exited, #{code => Code, signal => binary_to_atom(SigBin, utf8)}}
    end;
decode_event(<<"task_oom">>, Msg) ->
    Code = maps:get(<<"exit_code">>, Msg, -1),
    {oom_killed, #{code => Code}};
decode_event(Other, _Msg) ->
    {other, Other}.

deliver(Id, Event, S) ->
    Subs = maps:get(Id, maps:get(subscribers, S), []),
    case Subs of
        [] -> push_buffer(Id, Event, S);
        _ ->
            lists:foreach(fun(P) -> P ! {ctrd_event, Id, Event} end, Subs),
            S
    end.

push_buffer(Id, Event, S) ->
    Buffers = maps:get(buffers, S),
    Buf = maps:get(Id, Buffers, []),
    Buf1 = [Event | Buf],
    Buf2 = case length(Buf1) > ?BUFFER_LIMIT of
               true -> evict_sticky_terminal(Buf1);
               false -> Buf1
           end,
    S#{buffers := Buffers#{Id => Buf2}}.

evict_sticky_terminal(Buf) ->
    Rev = lists:reverse(Buf),
    case drop_first_nonterminal(Rev, []) of
        {ok, Rest} -> lists:reverse(Rest);
        none -> lists:reverse(tl(Rev))
    end.

drop_first_nonterminal([], _Acc) -> none;
drop_first_nonterminal([E | R], Acc) ->
    case is_terminal(E) of
        false -> {ok, lists:reverse(Acc, R)};
        true  -> drop_first_nonterminal(R, [E | Acc])
    end.

is_terminal({exited, _})           -> true;
is_terminal({oom_killed, _})       -> true;
is_terminal({failed_permanent, _}) -> true;
is_terminal(_)                     -> false.

simple_ack({ok, _})       -> ok;
simple_ack({error, Err})  -> {error, Err}.

items_or_empty(null)             -> [];
items_or_empty(L) when is_list(L) -> L.

ns() ->
    application:get_env(node_agent, ctrd_namespace, <<"mycelium">>).
