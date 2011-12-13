%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% a PN-Counter CRDT
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>

-module(riak_crdt_pncounter).

-behaviour(riak_crdt).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
    riak_crdt_gcounter:equal(Incr1, Incr2) andalso riak_crdt_gcounter:equal(Decr1, Decr2).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({[], []}, new()).

value_test() ->
    PNCnt1 = {[{1, 1}, {2, 13}, {3, 1}], [{2, 10}, {4, 1}]},
    PNCnt2 = {[], []},
    PNCnt3 = {[{1, 3}, {2, 1}, {3, 1}], [{1, 3}, {2, 1}, {3, 1}]},
    ?assertEqual(4, value(PNCnt1)),
    ?assertEqual(0, value(PNCnt2)),
    ?assertEqual(0, value(PNCnt3)).

update_increment_test() ->
    PNCnt0 = new(),
    PNCnt1 = update(increment, 1, PNCnt0),
    PNCnt2 = update(increment, 2, PNCnt1),
    PNCnt3 = update(increment, 1, PNCnt2),
    ?assertEqual({[{1, 2}, {2, 1}], []}, PNCnt3).

update_increment_by_test() ->
    PNCnt0 = new(),
    PNCnt1 = update({increment, 7}, 1, PNCnt0),
    ?assertEqual({[{1, 7}], []}, PNCnt1).

update_decrement_test() ->
    PNCnt0 = new(),
    PNCnt1 = update(increment, 1, PNCnt0),
    PNCnt2 = update(increment, 2, PNCnt1),
    PNCnt3 = update(increment, 1, PNCnt2),
    PNCnt4 = update(decrement, 1, PNCnt3),
    ?assertEqual({[{1, 2}, {2, 1}], [{1, 1}]}, PNCnt4).

update_decrement_by_test() ->
    PNCnt0 = new(),
    PNCnt1 = update({increment, 7}, 1, PNCnt0),
    PNCnt2 = update({decrement, 5}, 1, PNCnt1),
    ?assertEqual({[{1, 7}], [{1, 5}]}, PNCnt2).

merge_test() ->
    PNCnt1 = {[{<<"1">>, 1},
               {<<"2">>, 2},
               {<<"4">>, 4}], []},
    PNCnt2 = {[{<<"3">>, 3},
               {<<"4">>, 3}], []},
    ?assertEqual({[], []}, merge(new(), new())),
    ?assertEqual({[{<<"1">>,1},{<<"2">>,2},{<<"4">>,4},{<<"3">>,3}], []},
                merge(PNCnt1, PNCnt2)).

merge_too_test() ->
    PNCnt1 = {[{<<"5">>, 5}], [{<<"7">>, 4}]},
    PNCnt2 = {[{<<"6">>, 6}, {<<"7">>, 7}], [{<<"5">>, 2}]},
    ?assertEqual({[{<<"5">>, 5},{<<"6">>,6}, {<<"7">>, 7}], [{<<"7">>, 4}, {<<"5">>, 2}]},
                 merge(PNCnt1, PNCnt2)).

equal_test() ->
    PNCnt1 = {[{1, 2}, {2, 1}, {4, 1}], [{1, 1}, {3, 1}]},
    PNCnt2 = {[{1, 1}, {2, 4}, {3, 1}], []},
    PNCnt3 = {[{1, 2}, {2, 1}, {4, 1}], [{3, 1}, {1, 1}]},
    PNCnt4 = {[{4, 1}, {1, 2}, {2, 1}], [{1, 1}, {3, 1}]},
    ?assertNot(equal(PNCnt1, PNCnt2)),
    ?assert(equal(PNCnt3, PNCnt4)),
    ?assert(equal(PNCnt1, PNCnt3)).

usage_test() ->
    PNCnt1 = new(),
    PNCnt2 = new(),
    ?assert(equal(PNCnt1, PNCnt2)),
    PNCnt1_1 = update({increment, 2}, a1, PNCnt1),
    PNCnt2_1 = update(increment, a2, PNCnt2),
    PNCnt3 = merge(PNCnt1_1, PNCnt2_1),
    PNCnt2_2 = update({increment, 3}, a3, PNCnt2_1),
    PNCnt3_1 = update(increment, a4, PNCnt3),
    PNCnt3_2 = update(increment, a1, PNCnt3_1),
    PNCnt3_3 = update({decrement, 2}, a5, PNCnt3_2),
    PNCnt2_3 = update(decrement, a2, PNCnt2_2),
    ?assertEqual({[{a1, 3}, {a4, 1}, {a2, 1}, {a3, 3}], [{a5, 2}, {a2, 1}]},
                 merge(PNCnt3_3, PNCnt2_3)).

-endif.
