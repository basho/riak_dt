%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% a PN-Counter CRDT
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>

-module(riak_crdt_pncounter).

-behaviour(riak_crdt).

%% API
-export([new/0, value/1, update/3, merge/2]).

new() ->
    {vclock:fresh(), vclock:fresh()}.

value({Incr, Decr}) ->
    sum(Incr) - sum(Decr).

update(increment, Actor, {Incr, Decr}) ->
    {vclock:increment(Actor, Incr), Decr};
update(decrement, Actor, {Incr, Decr}) ->
    {Incr, vclock:increment(Actor, Decr)}.
    
merge({Incr1, Decr1}, {Incr2, Decr2}) ->   
    MergedIncr = vclock:merge([Incr1, Incr2]),
    MergedDecr = vclock:merge([Decr1, Decr2]),
    {MergedIncr, MergedDecr}.

%% Private
sum([]) ->
    0;
sum(Vclock) ->
    lists:sum([vclock:get_counter(Node, Vclock) || Node <- vclock:all_nodes(Vclock)]).
