-module(riak_crdt_demo).

-export([part/0, heal/0]).

part() ->
    true = rpc:call('riak_crdt2@127.0.0.1', erlang, set_cookie, ['riak_crdt2@127.0.0.1', riak_crdt2]),
    true = erlang:set_cookie(node(), riak_crdt2),
    true = rpc:call('riak_crdt2@127.0.0.1', erlang, disconnect_node, ['riak_crdt3@127.0.0.1']),
    true = rpc:call('riak_crdt2@127.0.0.1', erlang, disconnect_node, ['riak_crdt4@127.0.0.1']),
    true = erlang:disconnect_node('riak_crdt3@127.0.0.1'),
    true = erlang:disconnect_node('riak_crdt4@127.0.0.1').


heal() ->
    true = rpc:call('riak_crdt2@127.0.0.1', erlang, set_cookie, ['riak_crdt2@127.0.0.1', riak_crdt]),
    true = erlang:set_cookie(node(), riak_crdt).
