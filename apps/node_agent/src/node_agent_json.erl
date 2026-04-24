%% node_agent_json: minimal JSON codec for the helper protocol.
%%
%% Handles the subset we actually send and receive: objects with atom
%% or binary keys, strings (binaries), integers, floats, booleans, null,
%% arrays, nested objects. Emits binaries; decode returns maps with
%% binary keys.
%%
%% This is intentionally small — the external surface of the protocol
%% is narrow and well-controlled on both sides. When the protocol grows
%% (or we need perf), swap in thoas or jsone.
-module(node_agent_json).

-export([encode/1, decode/1]).

-spec encode(term()) -> binary().
encode(V) ->
    iolist_to_binary(enc(V)).

enc(null)             -> <<"null">>;
enc(true)             -> <<"true">>;
enc(false)            -> <<"false">>;
enc(V) when is_integer(V) -> integer_to_binary(V);
enc(V) when is_float(V)   -> float_to_binary(V, [{decimals, 6}, compact]);
enc(V) when is_binary(V)  -> [$", esc(V), $"];
enc(V) when is_atom(V)    -> [$", esc(atom_to_binary(V, utf8)), $"];
enc(V) when is_list(V) ->
    [$[, join_enc(V), $]];
enc(V) when is_map(V) ->
    Pairs = [[enc_key(K), $:, enc(Val)] || {K, Val} <- maps:to_list(V)],
    [${, lists:join($,, Pairs), $}].

enc_key(K) when is_atom(K)   -> [$", esc(atom_to_binary(K, utf8)), $"];
enc_key(K) when is_binary(K) -> [$", esc(K), $"].

join_enc([])      -> [];
join_enc([X])     -> [enc(X)];
join_enc([X | T]) -> [enc(X), $, | join_enc(T)].

esc(B) -> esc(B, <<>>).
esc(<<>>, Acc)                -> Acc;
esc(<<$\\, R/binary>>, Acc)   -> esc(R, <<Acc/binary, "\\\\">>);
esc(<<$", R/binary>>, Acc)    -> esc(R, <<Acc/binary, "\\\"">>);
esc(<<$\n, R/binary>>, Acc)   -> esc(R, <<Acc/binary, "\\n">>);
esc(<<$\r, R/binary>>, Acc)   -> esc(R, <<Acc/binary, "\\r">>);
esc(<<$\t, R/binary>>, Acc)   -> esc(R, <<Acc/binary, "\\t">>);
esc(<<C, R/binary>>, Acc) when C < 32 ->
    H = list_to_binary(io_lib:format("\\u~4.16.0b", [C])),
    esc(R, <<Acc/binary, H/binary>>);
esc(<<C, R/binary>>, Acc)     -> esc(R, <<Acc/binary, C>>).

-spec decode(binary()) -> {ok, term()} | {error, term()}.
decode(Bin) when is_binary(Bin) ->
    try
        {V, Rest} = parse(skip_ws(Bin)),
        case skip_ws(Rest) of
            <<>> -> {ok, V};
            _ -> {error, trailing_data}
        end
    catch
        throw:{parse_error, R} -> {error, R}
    end.

skip_ws(<<C, R/binary>>) when C =:= $\s; C =:= $\t; C =:= $\n; C =:= $\r ->
    skip_ws(R);
skip_ws(B) -> B.

parse(<<$", R/binary>>) -> parse_string(R, <<>>);
parse(<<${, R/binary>>) -> parse_object(skip_ws(R), #{});
parse(<<$[, R/binary>>) -> parse_array(skip_ws(R), []);
parse(<<"true", R/binary>>)  -> {true, R};
parse(<<"false", R/binary>>) -> {false, R};
parse(<<"null", R/binary>>)  -> {null, R};
parse(<<C, _/binary>> = B) when C =:= $-; C >= $0, C =< $9 ->
    parse_number(B);
parse(_) -> throw({parse_error, unexpected}).

parse_string(<<$", R/binary>>, Acc) -> {Acc, R};
parse_string(<<$\\, $", R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $">>);
parse_string(<<$\\, $\\, R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $\\>>);
parse_string(<<$\\, $/, R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $/>>);
parse_string(<<$\\, $n, R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $\n>>);
parse_string(<<$\\, $r, R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $\r>>);
parse_string(<<$\\, $t, R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $\t>>);
parse_string(<<$\\, $b, R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $\b>>);
parse_string(<<$\\, $f, R/binary>>, Acc) -> parse_string(R, <<Acc/binary, $\f>>);
parse_string(<<$\\, $u, A,B,C,D, R/binary>>, Acc) ->
    CP = list_to_integer([A,B,C,D], 16),
    parse_string(R, <<Acc/binary, CP/utf8>>);
parse_string(<<C, R/binary>>, Acc) ->
    parse_string(R, <<Acc/binary, C>>);
parse_string(<<>>, _) -> throw({parse_error, unterminated_string}).

parse_object(<<$}, R/binary>>, Acc) -> {Acc, R};
parse_object(<<$", R/binary>>, Acc) ->
    {K, R1} = parse_string(R, <<>>),
    R2 = expect_colon(skip_ws(R1)),
    {V, R3} = parse(skip_ws(R2)),
    parse_object_tail(skip_ws(R3), Acc#{K => V});
parse_object(_, _) -> throw({parse_error, bad_object}).

parse_object_tail(<<$}, R/binary>>, Acc) -> {Acc, R};
parse_object_tail(<<$,, R/binary>>, Acc) -> parse_object(skip_ws(R), Acc);
parse_object_tail(_, _) -> throw({parse_error, bad_object_tail}).

expect_colon(<<$:, R/binary>>) -> R;
expect_colon(_) -> throw({parse_error, expected_colon}).

parse_array(<<$], R/binary>>, Acc) -> {lists:reverse(Acc), R};
parse_array(B, Acc) ->
    {V, R} = parse(B),
    parse_array_tail(skip_ws(R), [V | Acc]).

parse_array_tail(<<$], R/binary>>, Acc) -> {lists:reverse(Acc), R};
parse_array_tail(<<$,, R/binary>>, Acc) -> parse_array(skip_ws(R), Acc);
parse_array_tail(_, _) -> throw({parse_error, bad_array_tail}).

parse_number(B) ->
    {NumBin, Rest} = take_number(B, <<>>),
    Num = case binary:match(NumBin, [<<".">>, <<"e">>, <<"E">>]) of
              nomatch -> binary_to_integer(NumBin);
              _       -> binary_to_float(NumBin)
          end,
    {Num, Rest}.

take_number(<<C, R/binary>>, Acc)
  when C >= $0, C =< $9; C =:= $-; C =:= $+; C =:= $.; C =:= $e; C =:= $E ->
    take_number(R, <<Acc/binary, C>>);
take_number(B, Acc) ->
    {Acc, B}.
