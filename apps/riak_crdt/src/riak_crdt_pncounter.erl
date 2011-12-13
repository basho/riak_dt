%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% a PN-Counter CRDT
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>

-module(riak_crdt_pncounter).

-behaviour(riak_crdt).

%% API
-export([new/0, value/1, update/3, merge/2, equal/2]).

new() ->
    {riak_crdt_gcounter:new(), riak_crdt_gcounter:new()}.

value({Incr, Decr}) ->
    riak_crdt_gcounter:value(Incr) -  riak_crdt_gcounter:value(Decr).

update(increment, Actor, {Incr, Decr}) ->
    {riak_crdt_gcounter:update(increment, Actor, Incr), Decr};
update({increment, By}, Actor, {Incr, Decr}) when is_integer(By), By > 0 ->
    {riak_crdt_gcounter:update({increment, By}, Actor, Incr), Decr};
update(decrement, Actor, {Incr, Decr}) ->
    {Incr, riak_crdt_gcounter:update(increment, Actor, Decr)};
update({decrement, By}, Actor, {Incr, Decr}) ->
    {Incr, riak_crdt_gcounter:update({increment, By}, Actor, Decr)}.
    
merge({Incr1, Decr1}, {Incr2, Decr2}) ->   
    MergedIncr =  riak_crdt_gcounter:merge(Incr1, Incr2),
    MergedDecr =  riak_crdt_gcounter:merge(Decr1, Decr2),
    {MergedIncr, MergedDecr}.

equal({Incr1, Decr1}, {Incr2, Decr2}) ->
    riak_crdt_gcounter:equal(Incr1, Incr2) andalso  riak_crdt_gcounter:equal(Decr1, Decr2).

