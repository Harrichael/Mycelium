-module(node_agent_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% rest_for_one: if ctrd_sup dies, dependent task_sup and adopter
    %% must be restarted so the re-adoption choreography re-runs.
    SupFlags = #{strategy  => rest_for_one,
                 intensity => 5,
                 period    => 30},
    Children = [
        #{id    => node_agent_scheduler_stub,
          start => {node_agent_scheduler_stub, start_link, []},
          type  => worker},
        #{id    => node_agent_registry,
          start => {node_agent_registry, start_link, []},
          type  => worker},
        #{id    => node_agent_ctrd_sup,
          start => {node_agent_ctrd_sup, start_link, []},
          type  => supervisor},
        #{id    => node_agent_task_sup,
          start => {node_agent_task_sup, start_link, []},
          type  => supervisor},
        #{id       => node_agent_adopter,
          start    => {node_agent_adopter, start_link, []},
          restart  => transient,
          type     => worker}
    ],
    {ok, {SupFlags, Children}}.
