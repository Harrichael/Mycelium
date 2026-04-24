-module(node_agent_task_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [restart_never_clean_exit,
     restart_never_failed_permanent,
     on_failure_then_permanent,
     on_failure_clean_exit_terminates,
     stop_task_no_restart_even_with_always,
     stop_watchdog_fires_when_no_exit,
     oom_triggers_restart_when_always,
     create_error_emits_failed_permanent].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    ok = application:load(node_agent),
    application:set_env(node_agent, ctrd_mod, node_agent_ctrd_fake),
    {ok, _} = application:ensure_all_started(node_agent),
    Config.

end_per_testcase(_Name, _Config) ->
    _ = application:stop(node_agent),
    _ = application:unload(node_agent),
    ok.

%% --- helpers ---------------------------------------------------------

base_spec(Id, Restart) ->
    #{id      => Id,
      image   => <<"docker.io/library/busybox:latest">>,
      cmd     => [<<"sh">>, <<"-c">>, <<"sleep 1">>],
      restart => Restart}.

start(Spec) ->
    {ok, Pid} = node_agent:start_task(Spec),
    await_started(maps:get(id, Spec)),
    Pid.

await_started(Id) ->
    ok = retry(50, fun() ->
        case [E || {I, {started, _}} = _T <- node_agent_scheduler_stub:history(),
                   E <- [_T], I =:= Id] of
            [] -> again;
            _  -> ok
        end
    end).

wait_dead(Pid) ->
    MRef = erlang:monitor(process, Pid),
    receive {'DOWN', MRef, process, Pid, _} -> ok
    after 5_000 -> error(task_did_not_die)
    end.

retry(0, _F) -> error(retry_exhausted);
retry(N, F) ->
    case F() of
        ok    -> ok;
        again ->
            timer:sleep(20),
            retry(N - 1, F)
    end.

history_for(Id) ->
    [E || {TaskId, E} <- node_agent_scheduler_stub:history(), TaskId =:= Id].

calls_of(Op) ->
    [Args || {O, Args} <- node_agent_ctrd_fake:calls(), O =:= Op].

%% --- tests -----------------------------------------------------------

restart_never_clean_exit(_Config) ->
    Id = <<"t1">>,
    Pid = start(base_spec(Id, never)),
    ok = node_agent_ctrd_fake:inject_exit(Id, 0),
    wait_dead(Pid),
    H = history_for(Id),
    ?assertMatch([{started, _}, {exited, #{code := 0}}], H).

restart_never_failed_permanent(_Config) ->
    Id = <<"t2">>,
    Pid = start(base_spec(Id, never)),
    ok = node_agent_ctrd_fake:inject_exit(Id, 7),
    wait_dead(Pid),
    H = history_for(Id),
    ?assertMatch([{started, _},
                  {exited, #{code := 7}},
                  {failed_permanent, #{reason := never}}], H).

on_failure_then_permanent(_Config) ->
    Id = <<"t3">>,
    Pid = start(base_spec(Id, {on_failure, 2, 60})),
    %% 3 failures: first two restart, third trips the fuse.
    node_agent_ctrd_fake:inject_exit(Id, 9),
    node_agent_ctrd_fake:flush(Pid),
    await_started(Id),
    node_agent_ctrd_fake:inject_exit(Id, 9),
    node_agent_ctrd_fake:flush(Pid),
    await_started(Id),
    node_agent_ctrd_fake:inject_exit(Id, 9),
    wait_dead(Pid),
    H = history_for(Id),
    ?assert(lists:any(
              fun({failed_permanent, #{reason := too_many_restarts}}) -> true; (_) -> false end,
              H)),
    %% ctrd:create called 3 times (initial + 2 restarts).
    CreateCalls = calls_of(create),
    ?assertEqual(3, length(CreateCalls)).

on_failure_clean_exit_terminates(_Config) ->
    Id = <<"t4">>,
    Pid = start(base_spec(Id, {on_failure, 3, 60})),
    ok = node_agent_ctrd_fake:inject_exit(Id, 0),
    wait_dead(Pid),
    H = history_for(Id),
    ?assertMatch([{started, _}, {exited, #{code := 0}}], H),
    ?assertNot(lists:any(fun({failed_permanent, _}) -> true; (_) -> false end, H)).

stop_task_no_restart_even_with_always(_Config) ->
    Id = <<"t5">>,
    Pid = start(base_spec(Id, always)),
    ok = node_agent:stop_task(Id, 1_000),
    wait_dead(Pid),
    H = history_for(Id),
    ?assertMatch([{started, _}, {exited, _}], H),
    ?assertNot(lists:any(fun({restarting, _}) -> true; (_) -> false end, H)),
    KillCalls = calls_of(kill),
    ?assertEqual(1, length(KillCalls)).

stop_watchdog_fires_when_no_exit(_Config) ->
    Id = <<"t6">>,
    Pid = start(base_spec(Id, always)),
    ok = node_agent_ctrd_fake:drop_next_exit(Id),
    ok = node_agent:stop_task(Id, 100),  %% watchdog fires at 100+2000 ms
    wait_dead(Pid),
    H = history_for(Id),
    ?assert(lists:any(
              fun({failed_permanent, #{reason := stop_watchdog_timeout}}) -> true;
                 (_) -> false end, H)),
    %% Registry cleaned up.
    ?assertEqual(undefined, node_agent_registry:whereis(Id)).

oom_triggers_restart_when_always(_Config) ->
    Id = <<"t7">>,
    Pid = start(base_spec(Id, always)),
    node_agent_ctrd_fake:inject_oom(Id, 137),
    node_agent_ctrd_fake:flush(Pid),
    await_started(Id),
    H = history_for(Id),
    ?assert(lists:any(
              fun({oom_killed, #{code := 137}}) -> true; (_) -> false end, H)),
    ?assert(lists:any(fun({restarting, _}) -> true; (_) -> false end, H)),
    %% Clean up: kill the task so we don't leak.
    ok = node_agent:stop_task(Id, 500),
    wait_dead(Pid).

create_error_emits_failed_permanent(_Config) ->
    Id = <<"t8">>,
    %% Pre-program a pull error for the first create.
    {ok, _} = application:ensure_all_started(node_agent),
    ok = node_agent_ctrd_fake:inject_pull_error(Id, image_not_found),
    {ok, Pid} = node_agent:start_task(base_spec(Id, never)),
    wait_dead(Pid),
    H = history_for(Id),
    ?assert(lists:any(
              fun({failed_permanent, #{reason := {create_failed, image_not_found}}}) -> true;
                 (_) -> false end, H)).
