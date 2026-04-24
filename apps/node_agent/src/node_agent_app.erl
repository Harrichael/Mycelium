-module(node_agent_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    CtrdMod = application:get_env(node_agent, ctrd_mod, node_agent_ctrd_real),
    persistent_term:put({node_agent, ctrd_mod}, CtrdMod),
    node_agent_sup:start_link().

stop(_State) ->
    persistent_term:erase({node_agent, ctrd_mod}),
    ok.
