%% node_agent_ctrd_sup: sub-supervisor for the configured executor.
%%
%% In stage 4+5 (against the fake) this starts just the fake gen_server.
%% In stage 7 the real-mode branch wires in node_agent_helper_port +
%% node_agent_ctrd_real under rest_for_one.
-module(node_agent_ctrd_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => rest_for_one, intensity => 5, period => 30},
    Mod = node_agent_ctrd:mod(),
    Children = children_for(Mod),
    {ok, {SupFlags, Children}}.

children_for(node_agent_ctrd_fake) ->
    [#{id    => node_agent_ctrd_fake,
       start => {node_agent_ctrd_fake, start_link, []},
       type  => worker}];
children_for(node_agent_ctrd_real) ->
    [#{id    => node_agent_helper_port,
       start => {node_agent_helper_port, start_link, []},
       type  => worker},
     #{id    => node_agent_ctrd_real,
       start => {node_agent_ctrd_real, start_link, []},
       type  => worker}];
children_for(Mod) ->
    %% Custom impls opt into this sup's supervision by exporting
    %% start_link/0.
    [#{id => Mod, start => {Mod, start_link, []}, type => worker}].
