%% -------------------------------------------------------------------
%%
%% riak_dt_pncounter: A convergent, replicated, state based PN counter
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_dt_pncounter).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, update/3, merge/2, equal/2]).
-export([gc_epoch/1, gc_ready/2, gc_get_fragment/2, gc_replace_fragment/3]).

-include("riak_dt_gc_meta.hrl").

-type pncounter() :: {riak_dt_gcounter:gcounter(),riak_dt_gcounter:gcounter()}.

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

%% EQC generator
-ifdef(EQC).
gen_op() ->
    oneof([increment, {increment, gen_pos()}, decrement, {decrement, gen_pos()} ]).

gen_pos()->
    ?LET(X, int(), 1+abs(X)).

update_expected(_ID, increment, Prev) ->
    Prev+1;
update_expected(_ID, decrement, Prev) ->
    Prev-1;
update_expected(_ID, {increment, By}, Prev) ->
    Prev+By;
update_expected(_ID, {decrement, By}, Prev) ->
    Prev-By;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.
-endif.

new() ->
    {riak_dt_gcounter:new(), riak_dt_gcounter:new()}.

value({Incr, Decr}) ->
    riak_dt_gcounter:value(Incr) -  riak_dt_gcounter:value(Decr).

update(increment, Actor, {Incr, Decr}) ->
    {riak_dt_gcounter:update(increment, Actor, Incr), Decr};
update({increment, By}, Actor, {Incr, Decr}) when is_integer(By), By > 0 ->
    {riak_dt_gcounter:update({increment, By}, Actor, Incr), Decr};
update(decrement, Actor, {Incr, Decr}) ->
    {Incr, riak_dt_gcounter:update(increment, Actor, Decr)};
update({decrement, By}, Actor, {Incr, Decr}) when is_integer(By), By > 0 ->
    {Incr, riak_dt_gcounter:update({increment, By}, Actor, Decr)}.

merge({Incr1, Decr1}, {Incr2, Decr2}) ->
    MergedIncr =  riak_dt_gcounter:merge(Incr1, Incr2),
    MergedDecr =  riak_dt_gcounter:merge(Decr1, Decr2),
    {MergedIncr, MergedDecr}.

equal({Incr1, Decr1}, {Incr2, Decr2}) ->
    riak_dt_gcounter:equal(Incr1, Incr2) andalso riak_dt_gcounter:equal(Decr1, Decr2).

%%% GC

-type gc_fragment() :: pncounter().

% We're ready to GC if either of the gcounters are ready to GC.
-spec gc_ready(gc_meta(), pncounter()) -> boolean().
gc_ready(Meta, {Inc,Dec}) ->
    riak_dt_gcounter:gc_ready(Meta, Inc)
        orelse riak_dt_gcounter:gc_ready(Meta,Dec).

-spec gc_epoch(pncounter()) -> riak_dt_gc:epoch().
gc_epoch({Inc,Dec}) ->
    Epoch = riak_dt_gcounter:gc_epoch(Inc),
    Epoch = riak_dt_gcounter:gc_epoch(Dec),
    Epoch.

-spec gc_get_fragment(gc_meta(), pncounter()) -> gc_fragment().
gc_get_fragment(Meta, {Inc,Dec}) ->
    IncFragment = riak_dt_gcounter:gc_get_fragment(Meta, Inc),
    DecFragment = riak_dt_gcounter:gc_get_fragment(Meta, Dec),
    {IncFragment, DecFragment}.

-spec gc_replace_fragment(gc_meta(), gc_fragment(), pncounter()) -> pncounter().
gc_replace_fragment(Meta, {IncFrag,DecFrag}, {Inc,Dec}) ->
    Inc1 = riak_dt_gcounter:gc_replace_fragment(Meta, IncFrag, Inc),
    Dec1 = riak_dt_gcounter:gc_replace_fragment(Meta, DecFrag, Dec),
    {Inc1,Dec1}.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(0, 1000, ?MODULE))]}.
-endif.

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
