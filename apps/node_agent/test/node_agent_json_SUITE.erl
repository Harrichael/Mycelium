-module(node_agent_json_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile(export_all).
-compile(nowarn_export_all).

suite() -> [{timetrap, {seconds, 10}}].

all() ->
    [roundtrip_scalars,
     roundtrip_nested,
     encode_atom_keys_and_values_as_strings,
     decode_string_escapes,
     decode_unicode_bmp_escape,
     decode_empty_object_and_array,
     decode_rejects_trailing_data,
     decode_rejects_unterminated_string,
     decode_rejects_missing_colon].

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.

%% --- tests ------------------------------------------------------------

roundtrip_scalars(_Config) ->
    Cases = [null, true, false,
             0, 1, -1, 1234567890,
             <<>>, <<"hello">>, <<"with space">>, <<"emoji 🦠"/utf8>>],
    lists:foreach(fun(V) ->
        {ok, V1} = node_agent_json:decode(node_agent_json:encode(V)),
        ?assertEqual(V, V1)
    end, Cases).

roundtrip_nested(_Config) ->
    %% Round-trip through the decoder's map-with-binary-keys shape.
    V = #{<<"id">>    => <<"web-1">>,
          <<"tags">>  => [<<"prod">>, <<"blue">>],
          <<"count">> => 3,
          <<"meta">>  => #{<<"owner">> => <<"team-x">>,
                           <<"draft">> => false,
                           <<"limits">> => [1, 2, 3]}},
    {ok, V1} = node_agent_json:decode(node_agent_json:encode(V)),
    ?assertEqual(V, V1).

encode_atom_keys_and_values_as_strings(_Config) ->
    %% The helper protocol uses atom keys for convenience on the Erlang
    %% side; they must serialize to JSON string keys/values.
    Bin = node_agent_json:encode(#{op => ping, id => <<"a">>}),
    {ok, Decoded} = node_agent_json:decode(Bin),
    ?assertEqual(<<"ping">>, maps:get(<<"op">>, Decoded)),
    ?assertEqual(<<"a">>, maps:get(<<"id">>, Decoded)).

decode_string_escapes(_Config) ->
    ?assertEqual({ok, <<"a\"b">>}, node_agent_json:decode(<<"\"a\\\"b\"">>)),
    ?assertEqual({ok, <<"a\\b">>}, node_agent_json:decode(<<"\"a\\\\b\"">>)),
    ?assertEqual({ok, <<"a\nb">>}, node_agent_json:decode(<<"\"a\\nb\"">>)),
    ?assertEqual({ok, <<"a\tb">>}, node_agent_json:decode(<<"\"a\\tb\"">>)),
    ?assertEqual({ok, <<"a\rb">>}, node_agent_json:decode(<<"\"a\\rb\"">>)),
    ?assertEqual({ok, <<"a/b">>},  node_agent_json:decode(<<"\"a\\/b\"">>)).

decode_unicode_bmp_escape(_Config) ->
    %% BMP character via \u escape (no surrogate pairs — those are
    %% tracked as a known limitation in issues.txt).
    ?assertEqual({ok, <<"café"/utf8>>},
                 node_agent_json:decode(<<"\"caf\\u00e9\"">>)).

decode_empty_object_and_array(_Config) ->
    ?assertEqual({ok, #{}}, node_agent_json:decode(<<"{}">>)),
    ?assertEqual({ok, []},  node_agent_json:decode(<<"[]">>)),
    ?assertEqual({ok, [[], #{}]}, node_agent_json:decode(<<"[[],{}]">>)).

decode_rejects_trailing_data(_Config) ->
    ?assertMatch({error, trailing_data},
                 node_agent_json:decode(<<"true false">>)).

decode_rejects_unterminated_string(_Config) ->
    ?assertMatch({error, unterminated_string},
                 node_agent_json:decode(<<"\"abc">>)).

decode_rejects_missing_colon(_Config) ->
    ?assertMatch({error, _},
                 node_agent_json:decode(<<"{\"k\" 1}">>)).
