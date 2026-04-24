%% mycelium_proto: shared types for Mycelium components.
%%
%% Importing component: the module is header-like; functions here are
%% for default-filling and cheap validation, not business logic.
%%
%% Example:
%%   Spec0 = #{id => <<"web-1">>,
%%             image => <<"docker.io/library/nginx:1.27">>,
%%             cmd => [<<"nginx">>, <<"-g">>, <<"daemon off;">>]},
%%   Spec  = mycelium_proto:task_spec_with_defaults(Spec0),
%%   ok    = mycelium_proto:validate_task_spec(Spec).
-module(mycelium_proto).

-export([task_spec_with_defaults/1,
         validate_task_spec/1,
         default_stop_timeout_ms/0,
         default_restart/0]).

-export_type([task_id/0,
              signal/0,
              cpu_max/0,
              mem_max/0,
              restart/0,
              task_spec/0,
              task_event/0]).

-type task_id() :: binary().
-type signal()  :: 'SIGTERM' | 'SIGKILL' | 'SIGINT' | 'SIGHUP'.
-type cpu_max() :: {Quota :: pos_integer(), Period :: pos_integer()}.
-type mem_max() :: pos_integer().

-type restart() :: never
                 | always
                 | {on_failure, MaxN :: non_neg_integer(), WindowSec :: pos_integer()}.

-type task_spec() :: #{id      := task_id(),
                       image   := binary(),
                       cmd     := [binary()],
                       env     => [binary()],
                       cwd     => binary(),
                       cpu_max => cpu_max(),
                       mem_max => mem_max(),
                       restart => restart(),
                       stop_timeout_ms => pos_integer()}.

-type task_event() :: {started,          #{pid := pos_integer()}}
                    | {exited,           #{code := integer(), signal => signal()}}
                    | {oom_killed,       #{code := integer()}}
                    | {restarting,       #{attempt := pos_integer()}}
                    | {failed_permanent, #{reason := term()}}.

-spec default_stop_timeout_ms() -> pos_integer().
default_stop_timeout_ms() -> 10_000.

-spec default_restart() -> restart().
default_restart() -> {on_failure, 3, 60}.

-spec task_spec_with_defaults(map()) -> task_spec().
task_spec_with_defaults(Spec) when is_map(Spec) ->
    Defaults = #{env => [],
                 cwd => <<"/">>,
                 restart => default_restart(),
                 stop_timeout_ms => default_stop_timeout_ms()},
    maps:merge(Defaults, Spec).

-spec validate_task_spec(map()) -> ok | {error, term()}.
validate_task_spec(Spec) when is_map(Spec) ->
    Required = [id, image, cmd],
    case [K || K <- Required, not is_map_key(K, Spec)] of
        [] -> validate_shape(Spec);
        Missing -> {error, {missing, Missing}}
    end;
validate_task_spec(_) ->
    {error, not_a_map}.

validate_shape(#{id := Id, image := Image, cmd := Cmd} = Spec) ->
    case {is_binary(Id), is_binary(Image), is_list(Cmd) andalso lists:all(fun is_binary/1, Cmd)} of
        {true, true, true} -> validate_restart(maps:get(restart, Spec, default_restart()));
        {false, _, _} -> {error, {bad_id, Id}};
        {_, false, _} -> {error, {bad_image, Image}};
        {_, _, false} -> {error, {bad_cmd, Cmd}}
    end.

validate_restart(never) -> ok;
validate_restart(always) -> ok;
validate_restart({on_failure, N, W}) when is_integer(N), N >= 0, is_integer(W), W > 0 -> ok;
validate_restart(Other) -> {error, {bad_restart, Other}}.
