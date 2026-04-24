%% node_agent_task: owns one container task's lifecycle on this host.
%%
%% One gen_server per task. It does not talk to containerd directly —
%% every runtime interaction goes through node_agent_ctrd:mod(), which
%% is either node_agent_ctrd_real (real containerd via Go helper) or
%% node_agent_ctrd_fake (tests). Event delivery is by `!` message so
%% neither implementation can deadlock against a mid-call subscriber.
%%
%% Restart is always delete+create+start (idempotent, eliminates the
%% exists-check). This costs a few ms per restart and buys us a much
%% simpler state model.
-module(node_agent_task).
-behaviour(gen_server).

-export([start_link/1, stop/2, spec/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([start_args/0]).

-type mode() :: create | adopt.
-type start_args() :: #{spec := mycelium_proto:task_spec(),
                        mode => mode(),
                        current_status => running | stopped | unknown}.

-type watchdog() :: {reference(), reference()} | undefined.
%% {TimerRef, Correlation}: TimerRef is what we cancel; Correlation is
%% the payload we match on, so stale messages racing a cancel are
%% silently discarded.

-record(st, {spec              :: mycelium_proto:task_spec(),
             id                :: mycelium_proto:task_id(),
             stopping = false  :: boolean(),
             exit_times = []   :: [integer()],
             attempts   = 0    :: non_neg_integer(),
             watchdog          :: watchdog()}).

-spec start_link(start_args()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

-spec stop(pid(), non_neg_integer()) -> ok.
stop(Pid, TimeoutMs) ->
    gen_server:cast(Pid, {stop, TimeoutMs}).

-spec spec(pid()) -> mycelium_proto:task_spec().
spec(Pid) ->
    gen_server:call(Pid, spec).

init(#{spec := Spec} = Args) ->
    Id = maps:get(id, Spec),
    Mode = maps:get(mode, Args, create),
    case node_agent_registry:register(Id) of
        ok ->
            Mod = node_agent_ctrd:mod(),
            ok = Mod:subscribe(self(), Id),
            case Mode of
                create ->
                    self() ! enter_create;
                adopt ->
                    Status = maps:get(current_status, Args, running),
                    self() ! {enter_adopt, Status}
            end,
            {ok, #st{spec = Spec, id = Id}};
        {error, {already_registered, Existing}} ->
            {stop, {already_registered, Id, Existing}}
    end.

handle_call(spec, _From, #st{spec = Spec} = S) ->
    {reply, Spec, S}.

handle_cast({stop, _Timeout}, #st{stopping = true} = S) ->
    {noreply, S};
handle_cast({stop, Timeout}, #st{id = Id} = S) ->
    Mod = node_agent_ctrd:mod(),
    Corr = erlang:make_ref(),
    Timer = erlang:send_after(Timeout + 2_000, self(), {stop_watchdog, Corr}),
    _ = Mod:kill(Id, 'SIGTERM', Timeout),
    {noreply, S#st{stopping = true, watchdog = {Timer, Corr}}}.

handle_info(enter_create, S) ->
    try_create_and_start(S);
handle_info({enter_adopt, Status}, S) ->
    enter_adopt(Status, S);

handle_info({ctrd_event, Id, Event}, #st{id = Id} = S) ->
    on_event(Event, S);

handle_info({stop_watchdog, Corr}, #st{watchdog = {_, Corr}, id = Id} = S) ->
    logger:warning("stop watchdog fired for ~s — container may be orphaned", [Id]),
    emit(Id, {failed_permanent, #{reason => stop_watchdog_timeout}}),
    {stop, normal, S#st{watchdog = undefined}};
handle_info({stop_watchdog, _Stale}, S) ->
    {noreply, S};

handle_info(_Other, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

%% ------------------------------------------------------------------

try_create_and_start(#st{spec = Spec, id = Id} = S) ->
    Mod = node_agent_ctrd:mod(),
    _ = Mod:delete(Id),
    case Mod:create(Spec) of
        ok ->
            case Mod:start(Id) of
                ok ->
                    {noreply, S};
                {error, StartReason} ->
                    emit(Id, {failed_permanent, #{reason => {start_failed, StartReason}}}),
                    {stop, normal, S}
            end;
        {error, CreateReason} ->
            emit(Id, {failed_permanent, #{reason => {create_failed, CreateReason}}}),
            {stop, normal, S}
    end.

enter_adopt(running, S) ->
    {noreply, S};
enter_adopt(unknown, #st{id = Id} = S) ->
    logger:warning("adopting ~s with unknown status; waiting for event", [Id]),
    {noreply, S};
enter_adopt(stopped, #st{id = Id} = S) ->
    self() ! {ctrd_event, Id, {exited, #{code => 0}}},
    {noreply, S}.

on_event({started, _} = Evt, #st{id = Id} = S) ->
    emit(Id, Evt),
    {noreply, S};
on_event({exited, _} = Evt, S) ->
    handle_exit_like(Evt, S);
on_event({oom_killed, _} = Evt, S) ->
    handle_exit_like(Evt, S);
on_event(_Other, S) ->
    {noreply, S}.

handle_exit_like(Evt, #st{stopping = true, id = Id, watchdog = W} = S) ->
    cancel_watchdog(W),
    emit(Id, Evt),
    {stop, normal, S#st{watchdog = undefined}};
handle_exit_like(Evt, #st{id = Id, spec = Spec} = S) ->
    Code = event_code(Evt),
    Restart = maps:get(restart, mycelium_proto:task_spec_with_defaults(Spec)),
    emit(Id, Evt),
    case decide_restart(Restart, Code, S) of
        {terminate, normal} ->
            {stop, normal, S};
        {terminate_permanent, Reason} ->
            emit(Id, {failed_permanent, #{reason => Reason}}),
            {stop, normal, S};
        {restart, S1} ->
            emit(Id, {restarting, #{attempt => S1#st.attempts}}),
            try_create_and_start(S1)
    end.

decide_restart(never, 0, _S) ->
    {terminate, normal};
decide_restart(never, _NonZero, _S) ->
    {terminate_permanent, never};
decide_restart(always, _Code, #st{attempts = N} = S) ->
    {restart, S#st{attempts = N + 1}};
decide_restart({on_failure, _Max, _Win}, 0, _S) ->
    {terminate, normal};
decide_restart({on_failure, MaxN, WindowSec}, _Code, #st{exit_times = Es, attempts = N} = S) ->
    Now = erlang:monotonic_time(second),
    Cutoff = Now - WindowSec,
    Recent = [T || T <- Es, T >= Cutoff],
    NewTimes = [Now | Recent],
    case length(NewTimes) > MaxN of
        true  -> {terminate_permanent, too_many_restarts};
        false -> {restart, S#st{exit_times = NewTimes, attempts = N + 1}}
    end.

event_code({exited, #{code := C}})     -> C;
event_code({oom_killed, #{code := C}}) -> C;
event_code(_) -> -1.

emit(Id, Event) ->
    node_agent_scheduler_stub:report(Id, Event).

cancel_watchdog(undefined) -> ok;
cancel_watchdog({Timer, _Corr}) ->
    _ = erlang:cancel_timer(Timer),
    ok.
