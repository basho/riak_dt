-module(riak_dt_wm_test).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).

-define(HTTP_PORT, 32767).
-define(URL, "http://127.0.0.1:32767").

setup(load) ->
    application:set_env(riak_core, http, [{"127.0.0.1", ?HTTP_PORT}]),
    {ok, Dir} = application:get_env(riak_dt, data_root),
    os:cmd("rm -rf " ++ Dir);
setup(_) ->
    ok.

pncounter_update_test_() ->
     {setup,
      fun() ->
              Result = riak_dt_test_util:setup("pncounter_update_test", fun setup/1),
              ok = riak_dt_test_util:wait_for_port(?HTTP_PORT, 5000),
              Result
      end,
      fun riak_dt_test_util:cleanup/1,
      {inparallel,
       [
             fun pncounter_increment/0,
             fun pncounter_decrement/0,
             fun pncounter_update_errors/0
       ]}
     }.

pncounter_increment() ->
    ?assertMatch({ok, {{_, 204, _}, _, _}}, request_counter("pnincr", "1")),
    ?assertMatch({ok, {{_, 200, _}, _, "1"}}, request_counter("pnincr")),
    ?assertMatch({ok, {{_, 204, _}, _, _}},  request_counter("pnincr", "5")),
    ?assertMatch({ok, {{_, 200, _}, _, "6"}}, request_counter("pnincr")).

pncounter_decrement() ->
    ?assertMatch({ok, {{_, 204, _}, _, _}}, request_counter("pndecr", "-1")),
    ?assertMatch({ok, {{_, 200, _}, _, "-1"}}, request_counter("pndecr")),
    ?assertMatch({ok, {{_, 204, _}, _, _}}, request_counter("pndecr", "-5")),
    ?assertMatch({ok, {{_, 200, _}, _, "-6"}}, request_counter("pndecr")).

pncounter_update_errors() ->
    ?assertMatch({ok, {{_, 400, _}, _, _}}, request_counter("pnup", "-abc")),
    ?assertMatch({ok, {{_, 400, _}, _, _}}, request_counter("pnup", "")).

request_counter(Key) ->
    lager:debug("Fetching value of counter ~s~n", [Key]),
    Result = httpc:request(?URL ++ "/counters/" ++ Key),
    lager:debug("GET ~s returned ~p~n", [Key, Result]),
    Result.

request_counter(Key, Body) ->
    lager:debug("Updating counter ~s with '~s'~n", [Key, Body]),
    Result = httpc:request(post, {?URL ++ "/counters/" ++ Key , [], "text/plain", Body}, [], []),
    lager:debug("POST ~s returned ~p~n", [Key, Result]),
    Result.
