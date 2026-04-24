%% node_agent_scheduler_stub: placeholder for the real scheduler.
%%
%% Exists to pin down the *event* protocol now so downstream code is
%% not coupled to the absence of a real scheduler. The transport is
%% trivial gen_server casts today; the wire shape is deliberately
%% stable so the future scheduler can be dropped in without touching
%% node_agent_task.
-module(node_agent_scheduler_stub).
-behaviour(gen_server).

-export([start_link/0, report/2, history/0, clear_history/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(SRV, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

-spec report(mycelium_proto:task_id(), mycelium_proto:task_event()) -> ok.
report(TaskId, Event) ->
    gen_server:cast(?SRV, {task_event, TaskId, Event}).

-spec history() -> [{mycelium_proto:task_id(), mycelium_proto:task_event()}].
history() ->
    gen_server:call(?SRV, history).

-spec clear_history() -> ok.
clear_history() ->
    gen_server:call(?SRV, clear_history).

init([]) ->
    {ok, #{events => []}}.

handle_call(history, _From, #{events := Es} = S) ->
    {reply, lists:reverse(Es), S};
handle_call(clear_history, _From, S) ->
    {reply, ok, S#{events := []}}.

handle_cast({task_event, TaskId, Event}, #{events := Es} = S) ->
    logger:info("scheduler_stub got ~p for ~s", [Event, TaskId]),
    {noreply, S#{events := [{TaskId, Event} | Es]}}.

handle_info(_, S) -> {noreply, S}.
