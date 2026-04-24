-module(node_agent_ctrd_real_SUITE).

%% Two tiers of tests live here:
%%
%%   * protocol_* — runs whenever the helper binary is present. Exercises
%%     node_agent_helper_port end-to-end (port spawn, JSON framing, op
%%     queueing, ack matching) using only the helper's built-in `ping`
%%     op. No containerd required.
%%
%%   * lifecycle_* — gated by CTRD_INTEGRATION=1 in the environment and
%%     a reachable containerd socket. Runs full start→exit cycles
%%     through the real node_agent_ctrd_real path. Skipped otherwise.
%%
%% Split into two groups so CI can opt into each independently.

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [{group, protocol},
     {group, lifecycle}].

groups() ->
    [{protocol, [sequence], [protocol_ping]},
     {lifecycle, [sequence], [lifecycle_start_exit]}].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_group(protocol, Config) ->
    case helper_binary_path() of
        {ok, _}    -> Config;
        not_found  -> {skip, helper_binary_not_built}
    end;
init_per_group(lifecycle, Config) ->
    case {helper_binary_path(), containerd_available(), integration_enabled()} of
        {{ok, _}, true, true} -> Config;
        _ -> {skip, "CTRD_INTEGRATION=1 and containerd required"}
    end.

end_per_group(_, _Config) -> ok.

init_per_testcase(_Name, Config) ->
    ok = application:load(node_agent),
    application:set_env(node_agent, ctrd_mod, node_agent_ctrd_real),
    {ok, Path} = helper_binary_path(),
    application:set_env(node_agent, helper_binary, Path),
    {ok, _} = application:ensure_all_started(node_agent),
    Config.

end_per_testcase(_Name, _Config) ->
    _ = application:stop(node_agent),
    _ = application:unload(node_agent),
    ok.

%% --- protocol tier ---------------------------------------------------

protocol_ping(_Config) ->
    {ok, #{<<"type">> := <<"ack">>, <<"op">> := <<"ping">>}} =
        node_agent_helper_port:send_op(#{op => ping}).

%% --- lifecycle tier --------------------------------------------------

lifecycle_start_exit(_Config) ->
    Id = iolist_to_binary(io_lib:format("ct-~p", [erlang:system_time(millisecond)])),
    Spec = #{id => Id,
             image => <<"docker.io/library/busybox:latest">>,
             cmd => [<<"sh">>, <<"-c">>, <<"echo hi; sleep 1">>],
             restart => never},
    {ok, Pid} = node_agent:start_task(Spec),
    MRef = erlang:monitor(process, Pid),
    receive {'DOWN', MRef, process, Pid, _} -> ok
    after 30_000 -> error(task_did_not_finish)
    end,
    H = [E || {TaskId, E} <- node_agent_scheduler_stub:history(), TaskId =:= Id],
    ?assert(lists:any(fun({started, _}) -> true; (_) -> false end, H)),
    ?assert(lists:any(fun({exited, _}) -> true; (_) -> false end, H)).

%% --- helpers ---------------------------------------------------------

helper_binary_path() ->
    %% node_agent's ebin lives at $PROJECT/_build/.../lib/node_agent/ebin.
    %% Walk up until we find `helpers/ctrd_helper/ctrd_helper` alongside.
    case code:lib_dir(node_agent) of
        {error, _} ->
            not_found;
        LibDir ->
            search_upward(LibDir, 10)
    end.

search_upward(_, 0) -> not_found;
search_upward(Dir, N) ->
    Candidate = filename:join([Dir, "helpers", "ctrd_helper", "ctrd_helper"]),
    case filelib:is_regular(Candidate) of
        true  -> {ok, Candidate};
        false ->
            Parent = filename:dirname(Dir),
            case Parent of
                Dir -> not_found;  %% reached root
                _   -> search_upward(Parent, N - 1)
            end
    end.

containerd_available() ->
    filelib:is_regular("/run/containerd/containerd.sock").

integration_enabled() ->
    case os:getenv("CTRD_INTEGRATION") of
        "1" -> true;
        _   -> false
    end.
