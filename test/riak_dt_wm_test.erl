-module(riak_dt_wm_test).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).

-define(HTTP_PORT, 32767).
-define(URL, "http://localhost:32767").

setup(load) ->
    application:set_env(riak_core, http, [{"127.0.0.1", ?HTTP_PORT}]),
    {ok, Dir} = application:get_env(riak_dt, data_root),
    os:cmd("rm -rf " ++ Dir);
setup(_) ->
    ok.

pncounter_update_test_() ->
    {setup,
     fun() ->
             riak_dt_test_util:setup("pncounter_update_test", fun setup/1)
     end,
     fun riak_dt_test_util:cleanup/1,
     [
      fun pncounter_increment/0,
      fun pncounter_decrement/0,
      fun pncounter_update_errors/0
     ]}.


pncounter_increment() ->
    ?assertMatch({ok, {{_, 204, _}, _, _}},
                 httpc:request(post, {?URL ++ "/counters/pnincr", [], "text/plain", "1"}, [], [])),
    ?assertMatch({ok, {{_, 200, _}, _, "1"}},
                 httpc:request(?URL ++ "/counters/pnincr")),
    ?assertMatch({ok, {{_, 204, _}, _, _}},
                 httpc:request(post, {?URL ++ "/counters/pnincr", [], "text/plain", "5"}, [], [])),
    ?assertMatch({ok, {{_, 200, _}, _, "6"}},
                 httpc:request(?URL ++ "/counters/pnincr")).

pncounter_decrement() ->
    ?assertMatch({ok, {{_, 204, _}, _, _}},
                 httpc:request(post, {?URL ++ "/counters/pndecr", [], "text/plain", "-1"}, [], [])),
    ?assertMatch({ok, {{_, 200, _}, _, "-1"}},
                 httpc:request(?URL ++ "/counters/pndecr")),
    ?assertMatch({ok, {{_, 204, _}, _, _}},
                 httpc:request(post, {?URL ++ "/counters/pndecr", [], "text/plain", "-5"}, [], [])),
    ?assertMatch({ok, {{_, 200, _}, _, "-6"}},
                 httpc:request(?URL ++ "/counters/pndecr")).

pncounter_update_errors() ->
    ?assertMatch({ok, {{_, 400, _}, _, _}},
                 httpc:request(post, {?URL ++ "/counters/pnup", [], "text/plain", "-abc"}, [], [])),
    ?assertMatch({ok, {{_, 400, _}, _, _}},
                 httpc:request(post, {?URL ++ "/counters/pnup", [], "text/plain", ""}, [], [])).
