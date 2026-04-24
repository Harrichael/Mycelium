%% node_agent_ctrd: behaviour for the container-runtime executor.
%%
%% Telos: the seam between task-lifecycle logic (node_agent_task) and
%% the actual container runtime. The real impl delegates to containerd
%% via a Go sidecar; the fake is pure Erlang for deterministic tests.
%% `node_agent_task` must be indifferent to which is wired in.
%%
%% Event delivery model: events are `!` messages, never gen_server
%% callbacks into the subscriber. This avoids deadlocks when a
%% subscriber is mid-call to the executor (e.g. an image-pull failure
%% delivered during `create/1`).
%%
%% `subscribe/2` is synchronous: on return, the pid is registered AND
%% any events buffered for this TaskId have already been delivered to
%% the subscriber's mailbox. This means a subscriber that does
%% `subscribe` then `create`+`start` will never miss early events.
-module(node_agent_ctrd).

-export([mod/0]).

-export_type([ctrd_list_item/0]).

-type ctrd_list_item() :: #{ id := mycelium_proto:task_id(),
                             status := running | stopped | unknown,
                             pid => pos_integer(),
                             spec := mycelium_proto:task_spec() }.

-callback create(mycelium_proto:task_spec()) -> ok | {error, term()}.
-callback start(mycelium_proto:task_id())    -> ok | {error, term()}.
-callback kill(mycelium_proto:task_id(), mycelium_proto:signal(),
               TimeoutMs :: non_neg_integer()) -> ok | {error, term()}.
-callback delete(mycelium_proto:task_id())   -> ok | {error, term()}.
-callback list()                             -> {ok, [ctrd_list_item()]} | {error, term()}.
-callback subscribe(Subscriber :: pid(), mycelium_proto:task_id()) -> ok.
-callback unsubscribe(Subscriber :: pid(), mycelium_proto:task_id()) -> ok.

%% Resolves the wired-in executor module (set by node_agent_app boot).
%% Tests swap this via `persistent_term:put({node_agent, ctrd_mod}, Mod)`.
-spec mod() -> module().
mod() ->
    persistent_term:get({node_agent, ctrd_mod}, node_agent_ctrd_real).
