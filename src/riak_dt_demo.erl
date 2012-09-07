-module(riak_dt_demo).

-export([part/0, heal/0]).

part() ->
    true = rpc:call('riak_dt2@127.0.0.1', erlang, set_cookie, ['riak_dt2@127.0.0.1', riak_dt2]),
    true = erlang:set_cookie(node(), riak_dt2),
    true = rpc:call('riak_dt2@127.0.0.1', erlang, disconnect_node, ['riak_dt3@127.0.0.1']),
    true = rpc:call('riak_dt2@127.0.0.1', erlang, disconnect_node, ['riak_dt4@127.0.0.1']),
    true = erlang:disconnect_node('riak_dt3@127.0.0.1'),
    true = erlang:disconnect_node('riak_dt4@127.0.0.1').


heal() ->
    true = rpc:call('riak_dt2@127.0.0.1', erlang, set_cookie, ['riak_dt2@127.0.0.1', riak_dt]),
    true = erlang:set_cookie(node(), riak_dt).
