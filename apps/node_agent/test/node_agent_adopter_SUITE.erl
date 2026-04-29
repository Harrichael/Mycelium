-module(node_agent_adopter_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [adopts_running_containers,
     adopts_stopped_synthesizes_exit,
     unknown_status_treated_as_running,
     skips_unadoptable_items,
     init_retry_succeeds_after_transient_failures,
     buffered_event_delivered_on_subscribe,
     sticky_terminal_eviction_preserves_exit,
     rest_for_one_reruns_adoption_after_ctrd_crash].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    ok = application:load(node_agent),
    application:set_env(node_agent, ctrd_mod, node_agent_ctrd_fake),
    _ = persistent_term:erase({node_agent_ctrd_fake, preload_items}),
    _ = persistent_term:erase({node_agent_ctrd_fake, list_fail_count}),
    Config.

end_per_testcase(_Name, _Config) ->
    _ = application:stop(node_agent),
    _ = application:unload(node_agent),
    _ = persistent_term:erase({node_agent_ctrd_fake, preload_items}),
    _ = persistent_term:erase({node_agent_ctrd_fake, list_fail_count}),
    ok.

%% --- helpers ---------------------------------------------------------

spec(Id) ->
    mycelium_proto:task_spec_with_defaults(#{
        id => Id,
        image => <<"docker.io/library/busybox:latest">>,
        cmd => [<<"sh">>, <<"-c">>, <<"sleep 1">>],
        restart => never
    }).

preload(Items) ->
    persistent_term:put({node_agent_ctrd_fake, preload_items}, Items).

set_list_fail_count(N) ->
    persistent_term:put({node_agent_ctrd_fake, list_fail_count}, N).

wait_adopter_done() ->
    ok = retry(200, fun() ->
        case erlang:whereis(node_agent_adopter) of
            undefined -> ok;
            _ -> again
        end
    end).

retry(0, _F) -> error(retry_exhausted);
retry(N, F) ->
    case F() of
        ok -> ok;
        again -> timer:sleep(10), retry(N - 1, F)
    end.

history_for(Id) ->
    [E || {TaskId, E} <- node_agent_scheduler_stub:history(), TaskId =:= Id].

adopter_calls() ->
    [C || {list, _} = C <- node_agent_ctrd_fake:calls()].

%% --- tests -----------------------------------------------------------

adopts_running_containers(_Config) ->
    S1 = spec(<<"a">>),
    S2 = spec(<<"b">>),
    preload([#{id => <<"a">>, status => running, pid => 100, spec => S1},
             #{id => <<"b">>, status => running, pid => 200, spec => S2}]),
    {ok, _} = application:ensure_all_started(node_agent),
    wait_adopter_done(),
    Tasks = node_agent:list_tasks(),
    ?assertEqual([<<"a">>, <<"b">>], lists:sort([Id || {Id, _} <- Tasks])),
    %% Adoption: no create calls (only list + subscribe).
    ?assertEqual([], [A || {create, A} <- node_agent_ctrd_fake:calls()]).

adopts_stopped_synthesizes_exit(_Config) ->
    S = spec(<<"c">>),
    preload([#{id => <<"c">>, status => stopped, spec => S}]),
    {ok, _} = application:ensure_all_started(node_agent),
    wait_adopter_done(),
    %% Task for 'c' has restart=never and the synthesized exit has code 0,
    %% so it terminates cleanly and is removed from the registry.
    ok = retry(200, fun() ->
        case node_agent_registry:whereis(<<"c">>) of
            undefined -> ok;
            _ -> again
        end
    end),
    H = history_for(<<"c">>),
    ?assert(lists:any(fun({exited, _}) -> true; (_) -> false end, H)).

unknown_status_treated_as_running(_Config) ->
    S = spec(<<"d">>),
    preload([#{id => <<"d">>, status => unknown, spec => S}]),
    {ok, _} = application:ensure_all_started(node_agent),
    wait_adopter_done(),
    ?assertMatch({ok, _}, node_agent_registry:whereis(<<"d">>)),
    %% Now emit a delayed exit event; the task should apply restart=never
    %% with non-zero code -> failed_permanent.
    ok = node_agent_ctrd_fake:inject_exit(<<"d">>, 9),
    ok = retry(200, fun() ->
        case node_agent_registry:whereis(<<"d">>) of
            undefined -> ok;
            _ -> again
        end
    end),
    H = history_for(<<"d">>),
    ?assert(lists:any(fun({failed_permanent, _}) -> true; (_) -> false end, H)).

skips_unadoptable_items(_Config) ->
    GoodSpec = spec(<<"good">>),
    BadSpec = #{id => <<"bad">>},                        %% missing required fields
    MismatchSpec = spec(<<"different-id">>),             %% spec id != list-item id
    preload([#{id => <<"good">>, status => running, spec => GoodSpec},
             #{id => <<"bad">>,  status => running, spec => BadSpec},
             #{id => <<"mismatch">>, status => running, spec => MismatchSpec}]),
    {ok, _} = application:ensure_all_started(node_agent),
    wait_adopter_done(),
    Tasks = [Id || {Id, _} <- node_agent:list_tasks()],
    ?assertEqual([<<"good">>], Tasks).

init_retry_succeeds_after_transient_failures(_Config) ->
    S = spec(<<"retry-me">>),
    preload([#{id => <<"retry-me">>, status => running, spec => S}]),
    set_list_fail_count(2),
    {ok, _} = application:ensure_all_started(node_agent),
    wait_adopter_done(),
    ?assertMatch({ok, _}, node_agent_registry:whereis(<<"retry-me">>)),
    %% list/0 should have been called 3 times (2 failures + 1 success).
    ?assertEqual(3, length(adopter_calls())).

buffered_event_delivered_on_subscribe(_Config) ->
    %% Drive the fake directly: event buffered pre-subscribe is flushed
    %% on subscribe.
    {ok, _} = node_agent_ctrd_fake:start_link(),
    try
        ok = node_agent_ctrd_fake:inject_exit(<<"buf">>, 3),
        ok = node_agent_ctrd_fake:subscribe(self(), <<"buf">>),
        receive
            {ctrd_event, <<"buf">>, {exited, #{code := 3}}} -> ok
        after 500 -> error(event_not_delivered)
        end
    after
        gen_server:stop(node_agent_ctrd_fake)
    end.

sticky_terminal_eviction_preserves_exit(_Config) ->
    {ok, _} = node_agent_ctrd_fake:start_link(),
    try
        Id = <<"stick">>,
        %% 17 non-terminals, then one terminal. Buffer holds 16; the
        %% sticky-terminal invariant means the terminal must survive.
        lists:foreach(
            fun(N) -> ok = node_agent_ctrd_fake:inject_started(Id, N) end,
            lists:seq(1, 17)
        ),
        ok = node_agent_ctrd_fake:inject_exit(Id, 42),
        ok = node_agent_ctrd_fake:subscribe(self(), Id),
        {Started, Exited} = count_events(0, 0),
        ?assert(Exited >= 1),
        %% Buffer is 16 total; after eviction we kept at least 1 exit.
        ?assert(Started + Exited =< 16)
    after
        gen_server:stop(node_agent_ctrd_fake)
    end.

count_events(St, Ex) ->
    receive
        {ctrd_event, _, {started, _}} -> count_events(St + 1, Ex);
        {ctrd_event, _, {exited, _}}  -> count_events(St, Ex + 1);
        {ctrd_event, _, _}            -> count_events(St, Ex)
    after 200 -> {St, Ex}
    end.

%% Verifies node_agent_sup's rest_for_one cascade: killing the ctrd
%% sub-supervisor must restart task_sup (flushing existing task gen_
%% servers) and adopter (re-running against the re-initialized fake).
%% The fake picks up its preload from persistent_term on restart, so the
%% post-cascade adopter re-registers the same Id — but to a new pid.
%% exit(_, kill) is brutal deliberately — normal exit wouldn't trigger
%% rest_for_one, so we need an abnormal exit to drive the cascade.
rest_for_one_reruns_adoption_after_ctrd_crash(_Config) ->
    S = spec(<<"re-adopt">>),
    preload([#{id => <<"re-adopt">>, status => running, spec => S}]),
    {ok, _} = application:ensure_all_started(node_agent),
    wait_adopter_done(),
    {ok, TaskPid1} = node_agent_registry:whereis(<<"re-adopt">>),

    CtrdSup = erlang:whereis(node_agent_ctrd_sup),
    ?assert(is_pid(CtrdSup)),
    MRef = erlang:monitor(process, CtrdSup),
    exit(CtrdSup, kill),
    receive {'DOWN', MRef, process, CtrdSup, _} -> ok
    after 5_000 -> error(ctrd_sup_did_not_die)
    end,

    %% Wait until the registry swaps in a new pid for the same Id — the
    %% strongest signal that the full cascade (ctrd_sup -> task_sup ->
    %% adopter) has completed and the adopter re-ran.
    ok = retry(200, fun() ->
        case node_agent_registry:whereis(<<"re-adopt">>) of
            {ok, P} when P =/= TaskPid1 -> ok;
            _ -> again
        end
    end),
    {ok, TaskPid2} = node_agent_registry:whereis(<<"re-adopt">>),
    ?assertNotEqual(TaskPid1, TaskPid2),
    %% Fake's call log is post-cascade only (it restarted with the rest).
    %% list/0 must appear (adopter ran) and no create/0 (running status
    %% means adopt, not create).
    Calls = node_agent_ctrd_fake:calls(),
    ?assert(lists:any(fun({list, _}) -> true; (_) -> false end, Calls)),
    ?assertEqual([], [A || {create, A} <- Calls]).
