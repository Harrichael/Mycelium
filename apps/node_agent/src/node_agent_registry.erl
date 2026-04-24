%% node_agent_registry: TaskId → pid directory, backed by a named ETS
%% table owned by this gen_server so the table survives any single
%% task gen_server crash (but not a registry crash, which is supposed
%% to propagate via the supervisor).
%%
%% Automatic cleanup: the registry monitors every registered pid and
%% drops the entry on DOWN. Callers don't need to deregister on normal
%% termination.
-module(node_agent_registry).
-behaviour(gen_server).

-export([start_link/0, register/1, register/2, whereis/1, all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TAB, ?MODULE).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register(mycelium_proto:task_id()) -> ok | {error, {already_registered, pid()}}.
register(TaskId) ->
    ?MODULE:register(TaskId, self()).

-spec register(mycelium_proto:task_id(), pid()) -> ok | {error, {already_registered, pid()}}.
register(TaskId, Pid) when is_binary(TaskId), is_pid(Pid) ->
    gen_server:call(?MODULE, {register, TaskId, Pid}).

-spec whereis(mycelium_proto:task_id()) -> {ok, pid()} | undefined.
whereis(TaskId) ->
    case ets:lookup(?TAB, TaskId) of
        [{_, Pid}] -> {ok, Pid};
        []         -> undefined
    end.

-spec all() -> [{mycelium_proto:task_id(), pid()}].
all() ->
    ets:tab2list(?TAB).

init([]) ->
    ?TAB = ets:new(?TAB, [named_table, protected, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, TaskId, Pid}, _From, State) ->
    case ets:lookup(?TAB, TaskId) of
        [{_, Existing}] when is_pid(Existing) ->
            case is_process_alive(Existing) of
                true  -> {reply, {error, {already_registered, Existing}}, State};
                false -> reregister(TaskId, Pid, State)
            end;
        [] ->
            reregister(TaskId, Pid, State)
    end.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, State) ->
    case maps:take(MRef, State) of
        {TaskId, State1} ->
            case ets:lookup(?TAB, TaskId) of
                [{_, Pid}] -> ets:delete(?TAB, TaskId);
                _          -> ok
            end,
            {noreply, State1};
        error ->
            {noreply, State}
    end;
handle_info(_Msg, State) ->
    {noreply, State}.

reregister(TaskId, Pid, State) ->
    MRef = erlang:monitor(process, Pid),
    ets:insert(?TAB, {TaskId, Pid}),
    {reply, ok, State#{MRef => TaskId}}.
