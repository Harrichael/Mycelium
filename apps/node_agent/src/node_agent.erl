%% node_agent: public API for the node agent.
%%
%% All callers (scheduler in the future, humans via rebar3 shell today)
%% should go through this module. It hides the supervision tree and
%% the runtime abstraction.
%%
%% Example:
%%   {ok, Pid} = node_agent:start_task(
%%     #{id    => <<"web-1">>,
%%       image => <<"docker.io/library/nginx:1.27">>,
%%       cmd   => [<<"nginx">>, <<"-g">>, <<"daemon off;">>]}),
%%   ok = node_agent:stop_task(<<"web-1">>).
-module(node_agent).

-export([start_task/1, stop_task/1, stop_task/2,
         list_tasks/0, whereis_task/1, drain/0, drain/1]).

-spec start_task(map()) -> {ok, pid()} | {error, term()}.
start_task(Spec0) ->
    case mycelium_proto:validate_task_spec(Spec0) of
        ok ->
            Spec = mycelium_proto:task_spec_with_defaults(Spec0),
            node_agent_task_sup:start_task(#{spec => Spec, mode => create});
        {error, _} = E -> E
    end.

-spec stop_task(mycelium_proto:task_id()) -> ok | {error, not_found}.
stop_task(TaskId) ->
    stop_task(TaskId, mycelium_proto:default_stop_timeout_ms()).

-spec stop_task(mycelium_proto:task_id(), non_neg_integer()) -> ok | {error, not_found}.
stop_task(TaskId, TimeoutMs) ->
    case node_agent_registry:whereis(TaskId) of
        {ok, Pid}   -> node_agent_task:stop(Pid, TimeoutMs);
        undefined   -> {error, not_found}
    end.

-spec list_tasks() -> [{mycelium_proto:task_id(), pid()}].
list_tasks() ->
    node_agent_registry:all().

-spec whereis_task(mycelium_proto:task_id()) -> {ok, pid()} | undefined.
whereis_task(TaskId) ->
    node_agent_registry:whereis(TaskId).

-spec drain() -> ok.
drain() ->
    drain(mycelium_proto:default_stop_timeout_ms()).

%% Drain every currently-running task concurrently and wait for all to
%% exit. Sequential op model in the helper means the real rate limit
%% is helper_kill_latency; this just parallelizes on the Erlang side.
-spec drain(non_neg_integer()) -> ok.
drain(TimeoutMs) ->
    Tasks = list_tasks(),
    Parent = self(),
    Refs = [spawn_drain_worker(Parent, Id, Pid, TimeoutMs) || {Id, Pid} <- Tasks],
    lists:foreach(fun await_drain/1, Refs),
    ok.

spawn_drain_worker(Parent, Id, Pid, TimeoutMs) ->
    Ref = erlang:make_ref(),
    spawn(fun() ->
        MonRef = erlang:monitor(process, Pid),
        node_agent_task:stop(Pid, TimeoutMs),
        receive
            {'DOWN', MonRef, process, Pid, _} -> ok
        after TimeoutMs + 5_000 ->
            logger:warning("drain: ~s did not exit within deadline", [Id])
        end,
        Parent ! {drain_done, Ref}
    end),
    Ref.

await_drain(Ref) ->
    receive {drain_done, Ref} -> ok end.
