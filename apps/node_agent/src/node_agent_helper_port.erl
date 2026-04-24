%% node_agent_helper_port: owns the OS port to the Go containerd helper.
%%
%% Telos: the *only* module that knows how to speak to the helper
%% process. Provides a sequential-op surface (send_op/1 returns only
%% after the helper acks), and routes unsolicited `event` frames to
%% node_agent_ctrd_real via `{helper_event, Event}` messages.
%%
%% Sequential guarantee: at most one op is in flight. Additional
%% callers are queued and dispatched FIFO as each ack arrives.
-module(node_agent_helper_port).
-behaviour(gen_server).

-export([start_link/0, send_op/1, send_op/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SRV, ?MODULE).
-define(DEFAULT_CALL_TIMEOUT_MS, 30_000).
-define(MAX_LINE, 1_048_576).

-record(st, {port            :: port() | undefined,
             pending         :: undefined | {reference(), gen_server:from(), map()},
             queue           :: queue:queue({reference(), gen_server:from(), map()}),
             event_sink      :: pid() | undefined,
             line_buf = <<>> :: binary()}).

start_link() ->
    gen_server:start_link({local, ?SRV}, ?MODULE, [], []).

-spec send_op(map()) -> {ok, map()} | {error, term()}.
send_op(Op) ->
    send_op(Op, ?DEFAULT_CALL_TIMEOUT_MS).

-spec send_op(map(), timeout()) -> {ok, map()} | {error, term()}.
send_op(Op, Timeout) ->
    gen_server:call(?SRV, {op, Op}, Timeout).

init([]) ->
    process_flag(trap_exit, true),
    Path = resolve_path(application:get_env(node_agent, helper_binary,
                                            "./helpers/ctrd_helper/ctrd_helper")),
    case file_exists(Path) of
        true ->
            Port = erlang:open_port({spawn_executable, Path},
                                    [{line, ?MAX_LINE}, binary, use_stdio,
                                     exit_status]),
            {ok, #st{port = Port, queue = queue:new()}};
        false ->
            {stop, {helper_binary_missing, Path}}
    end.

handle_call({op, _Op}, _From, #st{port = undefined} = S) ->
    {reply, {error, helper_not_running}, S};
handle_call({op, Op}, From, #st{pending = undefined} = S) ->
    Ref = erlang:make_ref(),
    dispatch(Op, Ref, S),
    {noreply, S#st{pending = {Ref, From, Op}}};
handle_call({op, Op}, From, S) ->
    Ref = erlang:make_ref(),
    {noreply, S#st{queue = queue:in({Ref, From, Op}, S#st.queue)}}.

handle_cast({register_event_sink, Pid}, S) ->
    {noreply, S#st{event_sink = Pid}};
handle_cast(_Msg, S) -> {noreply, S}.

handle_info({Port, {data, {eol, Line}}}, #st{port = Port, line_buf = Buf} = S) ->
    Full = <<Buf/binary, Line/binary>>,
    handle_line(Full, S#st{line_buf = <<>>});
handle_info({Port, {data, {noeol, Chunk}}}, #st{port = Port, line_buf = Buf} = S) ->
    {noreply, S#st{line_buf = <<Buf/binary, Chunk/binary>>}};
handle_info({Port, {exit_status, Status}}, #st{port = Port} = S) ->
    logger:error("ctrd_helper exited status=~p", [Status]),
    {stop, {helper_exited, Status}, S#st{port = undefined}};
handle_info({'EXIT', Port, Reason}, #st{port = Port} = S) ->
    {stop, {helper_port_exit, Reason}, S#st{port = undefined}};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #st{port = undefined}) -> ok;
terminate(_Reason, #st{port = Port}) ->
    catch erlang:port_close(Port),
    ok.

%% ----------------------------------------------------------------

dispatch(Op, _Ref, #st{port = Port}) ->
    Line = node_agent_json:encode(Op),
    Port ! {self(), {command, [Line, $\n]}}.

handle_line(Line, #st{} = S) ->
    case node_agent_json:decode(Line) of
        {ok, #{<<"type">> := <<"event">>} = Msg} ->
            route_event(Msg, S),
            {noreply, S};
        {ok, #{<<"type">> := Type} = Msg} when Type =:= <<"ack">>; Type =:= <<"error">> ->
            complete_pending(Msg, S);
        {ok, _Other} ->
            logger:warning("helper_port: unexpected msg ~p", [_Other]),
            {noreply, S};
        {error, Reason} ->
            logger:warning("helper_port: bad JSON line ~p (~p)", [Line, Reason]),
            {noreply, S}
    end.

route_event(Msg, #st{event_sink = undefined}) ->
    logger:warning("helper_port: event dropped (no sink): ~p", [Msg]);
route_event(Msg, #st{event_sink = Sink}) ->
    Sink ! {helper_event, Msg}.

complete_pending(Msg, #st{pending = undefined} = S) ->
    logger:warning("helper_port: stray response (no pending op): ~p", [Msg]),
    {noreply, S};
complete_pending(Msg, #st{pending = {_Ref, From, _Op}} = S) ->
    Reply = case Msg of
                #{<<"type">> := <<"ack">>} -> {ok, Msg};
                #{<<"type">> := <<"error">>} -> {error, Msg}
            end,
    gen_server:reply(From, Reply),
    drain_queue(S#st{pending = undefined}).

drain_queue(#st{queue = Q} = S) ->
    case queue:out(Q) of
        {empty, _} ->
            {noreply, S};
        {{value, {Ref, From, Op}}, Q1} ->
            dispatch(Op, Ref, S),
            {noreply, S#st{pending = {Ref, From, Op}, queue = Q1}}
    end.

resolve_path(Path) ->
    case filename:pathtype(Path) of
        absolute -> Path;
        _ ->
            %% Look for the binary relative to a handful of plausible
            %% roots so rebar3 shell, rebar3 ct, and release layouts all
            %% find it without configuration.
            case find_first_existing([Path,
                                      filename:join("../../..", Path),
                                      filename:join(code:priv_dir(node_agent), "ctrd_helper")])
            of
                {ok, P} -> P;
                error   -> Path
            end
    end.

find_first_existing([]) -> error;
find_first_existing([P | T]) ->
    case file_exists(P) of
        true -> {ok, P};
        false -> find_first_existing(T)
    end.

file_exists(Path) ->
    case filelib:is_regular(Path) of
        true  -> true;
        false -> false
    end.
