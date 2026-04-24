-module(node_agent_task_sup).
-behaviour(supervisor).

-export([start_link/0, start_task/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_task(node_agent_task:start_args()) ->
    {ok, pid()} | {error, term()}.
start_task(Args) ->
    supervisor:start_child(?MODULE, [Args]).

init([]) ->
    SupFlags = #{strategy  => simple_one_for_one,
                 intensity => 10,
                 period    => 60},
    Child = #{id       => node_agent_task,
              start    => {node_agent_task, start_link, []},
              restart  => temporary,
              shutdown => brutal_kill,
              type     => worker},
    {ok, {SupFlags, [Child]}}.
