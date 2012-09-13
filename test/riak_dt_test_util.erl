-module(riak_dt_test_util).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

-define(THUNK, fun(_) -> ok end).

setup(Test) ->
    setup(Test, ?THUNK).

setup(Test, Extra) ->
    Deps = resolve_deps(riak_dt),

    application:set_env(kernel, error_logger, silent),
    filelib:ensure_dir(filename:absname(Test ++ "/log/sasl.log")),
    application:set_env(sasl, sasl_error_logger, {file, Test++"/log/sasl.log"}),
    error_logger:tty(false),

    application:set_env(riak_core, ring_creation_size, 32),
    application:set_env(riak_core, ring_state_dir, Test ++ "/ring"),
    application:set_env(riak_core, platform_data_dir, Test ++ "/data"),
    application:set_env(riak_core, handoff_port, 0), %% pick a random handoff port
    application:set_env(lager, handlers, [{lager_file_backend,
                                           [
                                            {Test ++ "/log/debug.log", debug, 10485760, "$D0", 5}]}]),
    application:set_env(lager, crash_log, Test ++ "/log/crash.log"),

    application:set_env(riak_dt, data_root, filename:absname(Test ++ "/data/riak_dt_bitcask")),
    OldSettings = Extra(load),

    %% Start erlang node
    {ok, Hostname} = inet:gethostname(),
    TestNode = list_to_atom(Test ++ "@" ++ Hostname),
    net_kernel:start([TestNode, longnames]),

    [ application:start(A) || A <- Deps ],
    Extra(start),
    riak_core:wait_for_service(riak_dt),
    {Extra, OldSettings, Deps}.

cleanup({Extra, OldSettings, Deps}) ->
    [ application:stop(A) || A <- lists:reverse(Deps),
                             not is_otp_base_app(A) ],
    wait_for_application_shutdown(riak_dt),
    Extra({cleanup, OldSettings}),

    %% Stop distributed Erlang
    net_kernel:stop(),

    application:set_env(riak_core, vnode_modules, []),

    ok.

wait_for_application_shutdown(App) ->
    case lists:keymember(App, 1, application:which_applications()) of
        true ->
            timer:sleep(250),
            wait_for_application_shutdown(App);
        false ->
            ok
    end.

%% The following three functions build a list of dependent
%% applications. They will not handle circular or mutually-dependent
%% applications.
dep_apps(App) ->
    application:load(App),
    {ok, Apps} = application:get_key(App, applications),
    Apps.

all_deps(App, Deps) ->
    [[ all_deps(Dep, [App|Deps]) || Dep <- dep_apps(App),
                                    not lists:member(Dep, Deps)], App].

resolve_deps(App) ->
    DepList = all_deps(App, []),
    {AppOrder, _} = lists:foldl(fun(A,{List,Set}) ->
                                        case sets:is_element(A, Set) of
                                            true ->
                                                {List, Set};
                                            false ->
                                                {List ++ [A], sets:add_element(A, Set)}
                                        end
                                end,
                                {[], sets:new()},
                                lists:flatten(DepList)),
    AppOrder.

is_otp_base_app(kernel) -> true;
is_otp_base_app(stdlib) -> true;
is_otp_base_app(_) -> false.
