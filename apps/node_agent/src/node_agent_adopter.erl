%% node_agent_adopter: one-shot worker that reconstructs task
%% gen_servers from containerd's view of the world at startup.
%%
%% Lifecycle:
%%   start_link -> init returns {ok, _, {continue, adopt}}
%%   handle_continue(adopt) does the work, returns {stop, normal, _}
%%   transient restart: normal exit = no restart.
%%
%% On transient list/0 failures (helper still coming up), retries with
%% exponential backoff. On exhaustion, returns {stop, {list_failed, _}}
%% so the supervisor restarts the adopter under transient semantics.
%% If that keeps failing the intensity fuse of node_agent_sup trips,
%% which is the correct outcome — operating without adoption would
%% silently orphan every pre-existing container.
-module(node_agent_adopter).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_continue/2]).

-define(BACKOFFS_MS, [100, 250, 500, 1000, 2000]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #{}, {continue, adopt}}.

handle_continue(adopt, S) ->
    case list_with_retry(?BACKOFFS_MS) of
        {ok, Items} ->
            {Adopted, Skipped} = adopt_all(Items),
            logger:info("node_agent_adopter: adopted=~p skipped=~p", [Adopted, Skipped]),
            {stop, normal, S};
        {error, Reason} ->
            {stop, {list_failed, Reason}, S}
    end.

handle_call(_, _, S) -> {reply, ok, S}.
handle_cast(_, S)    -> {noreply, S}.
handle_info(_, S)    -> {noreply, S}.

%% --- internals -------------------------------------------------------

list_with_retry([]) ->
    (node_agent_ctrd:mod()):list();
list_with_retry([WaitMs | Rest]) ->
    Mod = node_agent_ctrd:mod(),
    case Mod:list() of
        {ok, _} = Ok -> Ok;
        {error, Reason} ->
            logger:warning("node_agent_adopter: list failed (~p); retrying in ~pms", [Reason, WaitMs]),
            timer:sleep(WaitMs),
            list_with_retry(Rest)
    end.

adopt_all(Items) ->
    lists:foldl(fun adopt_one/2, {0, 0}, Items).

adopt_one(Item, {A, Sk}) ->
    case validate_item(Item) of
        {ok, Spec, Status} ->
            Args = #{spec => Spec, mode => adopt, current_status => Status},
            case node_agent_task_sup:start_task(Args) of
                {ok, _Pid} ->
                    {A + 1, Sk};
                {error, Reason} ->
                    logger:warning("node_agent_adopter: ~s failed to adopt: ~p",
                                   [maps:get(id, Spec), Reason]),
                    {A, Sk + 1}
            end;
        {error, Reason} ->
            logger:warning("node_agent_adopter: unadoptable_container ~p: ~p",
                           [maps:get(id, Item, <<"(missing id)">>), Reason]),
            {A, Sk + 1}
    end.

validate_item(#{id := Id, status := Status, spec := Spec}) ->
    case mycelium_proto:validate_task_spec(Spec) of
        ok ->
            case maps:get(id, Spec) =:= Id of
                true  -> {ok, Spec, Status};
                false -> {error, {id_mismatch, Id, maps:get(id, Spec)}}
            end;
        {error, _} = E -> E
    end;
validate_item(Other) ->
    {error, {bad_item_shape, Other}}.
