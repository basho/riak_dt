%% -------------------------------------------------------------------
%%
%% riak_dt_gcounter: A state based, grow only, convergent counter
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc
%% A G-Counter CRDT. A G-Counter is a Grow-only counter. Modeled as a list of
%% two-tuples. Each entry in the list is an {actor, count} pair. The value of the counter
%% is the sum of all entries in the list. An actor may only update its own entry. An entry
%% can only be incremented. Borrows liberally from argv0 and Justin Sheehy's vclock module
%% in implementation.
%%
%% @see riak_kv_pncounter.erl for a counter that can be decremented
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(riak_dt_gcounter).

-behaviour(riak_dt).
-behaviour(riak_dt_gc).

-export([new/0, new/2, value/1, value/2, update/3, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).
-export([gc_epoch/1, gc_ready/2, gc_get_fragment/2, gc_replace_fragment/3]).

-include("riak_dt_gc_meta.hrl").


%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([gcounter/0, gcounter_op/0]).

-type gcounter() :: [{riak_dt:actor(),pos_integer()}].
-type gcounter_op() :: increment | {increment, pos_integer()}.

%% @doc Create a new, empty `gcounter()'
-spec new() -> gcounter().
new() ->
    orddict:new().

%% @doc Create a `gcounter()' with an initial update
-spec new(term(), pos_integer()) -> gcounter().
new(Id, Count) when is_integer(Count), Count > 0 ->
    update({increment, Count}, Id, new()).

%% @doc The single total value of a `gcounter()'.
-spec value(gcounter()) -> non_neg_integer().
value(GCnt) ->
    lists:sum([ Cnt || {_Act, Cnt} <- GCnt]).

%% @doc `value/2' is just `value/1' for a `gcounter()'
-spec value(any(), gcounter()) -> non_neg_integer().
value(_, GCnt) ->
    value(GCnt).

%% @doc `increment' the entry in `GCnt' for `Actor' by 1 or `{increment, Amt}'.
%% returns an updated `gcounter()' or error if `Amt' is not a `pos_integer()'
-spec update(gcounter_op(), term(), gcounter()) ->
                    gcounter().
update(increment, Actor, GCnt) ->
    increment_by(1, Actor, GCnt);
update({increment, Amount}, Actor, GCnt) when is_integer(Amount), Amount > 0 ->
    increment_by(Amount, Actor, GCnt).

%% @doc Merge two `gcounter()'s to a single `gcounter()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(gcounter(), gcounter()) -> gcounter().
merge(GCnt1, GCnt2) ->
    orddict:merge(fun(_, V1, V2) -> max(V1,V2) end,
                  GCnt1, GCnt2).

%% @doc Are two `gcounter()'s structurally equal? This is not `value/1' equality.
%% Two counters might represent the total `42', and not be `equal/2'. Equality here is
%% that both counters contain the same actors and those actors have the same count.
-spec equal(gcounter(), gcounter()) -> boolean().
equal(VA,VB) ->
    lists:sort(orddict:to_list(VA)) =:= lists:sort(orddict:to_list(VB)).

%%% GC

-type gc_fragment() :: gcounter().

-spec gc_ready(gc_meta(), gcounter()) -> boolean().
gc_ready(Meta, GCnt) ->
    GCActors = length([Act || {{gc, _Epoch}=Act,_Cnt} <- GCnt]),
    ROActors = length(ro_actors(Meta, GCnt)),
    TotalActors = length(GCnt),
    case TotalActors of
        0 -> false;
        1 -> (GCActors > 1) or ?SHOULD_GC(Meta, ROActors/TotalActors)
    end.

-spec gc_epoch(gcounter()) -> riak_dt_gc:epoch().
gc_epoch(GCnt) ->
    GCActors = [Act || {{gc, _Epoch}=Act,_Cnt} <- GCnt],
    {gc, Epoch} = hd(GCActors),
    Epoch.

-spec gc_get_fragment(gc_meta(), gcounter()) -> gc_fragment().
gc_get_fragment(Meta, GCnt) ->
    ROActors = ro_actors(Meta, GCnt),
    DeadActors = make_fragment(ROActors, GCnt, []),
    DeadActors.

-spec gc_replace_fragment(gc_meta(), gc_fragment(), gcounter()) -> gcounter().
gc_replace_fragment(Meta, DeadActors, GCnt0) ->
    Epoch = Meta?GC_META.epoch,
    {ActorAdd, GCnt1} = subtract_dead(DeadActors, GCnt0, 0),
    GCnt2 = prune_empty_nodes(GCnt1),
    [{{gc, Epoch},ActorAdd} | GCnt2].

% Returns the actors from GCnt that we won't get rid of during compaction.
ro_actors(Meta, GCnt) ->
    PAs = ordsets:from_list(Meta?GC_META.primary_actors),
    RoAs = ordsets:from_list(Meta?GC_META.readonly_actors),
    CounterActors = ordsets:from_list([Actor || {Actor, _} <- GCnt]),
    ordsets:intersection(ordsets:union(PAs, RoAs), CounterActors).

make_fragment(_ROActors, [], Dead) ->
    Dead;
% Ensure previous GCs are all removed.
make_fragment(ROActors, [{{gc, _Epoch}=Act,Cnt}|GCnt], Dead) ->
    make_fragment(ROActors, GCnt, [{Act,Cnt}|Dead]);
% And ensure any non-read-only actors are removed too
make_fragment(ROActors, [{Act,Cnt}|GCnt], Dead) ->
    case ordsets:is_element(Act, ROActors) of
        true ->  make_fragment(ROActors, GCnt, Dead);
        false -> make_fragment(ROActors, GCnt, [{Act,Cnt}|Dead])
    end.

subtract_dead([], GCnt, Sum) ->
    {Sum, GCnt};
subtract_dead([{Actor,Subtract}|Dead], GCnt, Sum) ->
    {Ctr, NewGCnt} = case lists:keytake(Actor, 1, GCnt) of
                        false ->
                            {-Subtract, GCnt};
                        {value, {_,C}, ModGCnt} ->
                            {C-Subtract, ModGCnt}
                     end,
    subtract_dead(Dead, [{Actor,Ctr}|NewGCnt], Sum+Subtract).

prune_empty_nodes(GCnt) ->
    [Pair || {_,Cnt}=Pair <- GCnt, Cnt =/= 0].

%% @private peform the increment.
-spec increment_by(pos_integer(), term(), gcounter()) -> gcounter().
increment_by(Amount, Actor, GCnt) when is_integer(Amount), Amount > 0 ->
    orddict:update_counter(Actor, Amount, GCnt).

-define(TAG, 70).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of a `gcounter()'
-spec to_binary(gcounter()) -> binary().
to_binary(GCnt) ->
    EntriesBin = term_to_binary(GCnt),
    <<?TAG:8/integer, ?V1_VERS:8/integer, EntriesBin/binary>>.

%% @doc Decode binary G-Counter
-spec from_binary(binary()) -> gcounter().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, EntriesBin/binary>>) ->
    binary_to_term(EntriesBin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
%% EQC generator
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

generate() ->
    ?LET(Ops, list(gen_op()),
         lists:foldl(fun(Op, Cntr) ->
                             riak_dt_gcounter:update(Op, choose(1, 50), Cntr) end,
                     riak_dt_gcounter:new(),
                     Ops)).

init_state() ->
    0.

gen_op() ->
    oneof([increment, {increment, gen_pos()}]).

gen_pos()->
    ?LET(X, int(), 1+abs(X)).

update_expected(_ID, increment, Prev) ->
    Prev+1;
update_expected(_ID, {increment, By}, Prev) ->
    Prev+By;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.
-endif.

new_test() ->
    ?assertEqual([], new()).

value_test() ->
    GC1 = [{1, 1}, {2, 13}, {3, 1}],
    GC2 = [],
    ?assertEqual(15, value(GC1)),
    ?assertEqual(0, value(GC2)).

update_increment_test() ->
    GC0 = new(),
    GC1 = update(increment, 1, GC0),
    GC2 = update(increment, 2, GC1),
    GC3 = update(increment, 1, GC2),
    ?assertEqual([{1, 2}, {2, 1}], GC3).

update_increment_by_test() ->
    GC0 = new(),
    GC = update({increment, 7}, 1, GC0),
    ?assertEqual([{1, 7}], GC).

merge_test() ->
    GC1 = [{<<"1">>, 1},
           {<<"2">>, 2},
           {<<"4">>, 4}],
    GC2 = [{<<"3">>, 3},
           {<<"4">>, 3}],
    ?assertEqual([], merge(new(), new())),
    ?assertEqual([{<<"1">>,1},{<<"2">>,2},{<<"3">>,3},{<<"4">>,4}],
                lists:sort( merge(GC1, GC2))).

merge_less_left_test() ->
    GC1 = [{<<"5">>, 5}],
    GC2 = [{<<"6">>, 6}, {<<"7">>, 7}],
    ?assertEqual([{<<"5">>, 5},{<<"6">>,6}, {<<"7">>, 7}],
                 merge(GC1, GC2)).

merge_less_right_test() ->
    GC1 = [{<<"6">>, 6}, {<<"7">>,7}],
    GC2 = [{<<"5">>, 5}],
    ?assertEqual([{<<"5">>,5},{<<"6">>,6}, {<<"7">>, 7}],
                 lists:sort( merge(GC1, GC2)) ).

merge_same_id_test() ->
    GC1 = [{<<"1">>, 2},{<<"2">>,4}],
    GC2 = [{<<"1">>, 3},{<<"3">>,5}],
    ?assertEqual([{<<"1">>, 3},{<<"2">>,4},{<<"3">>,5}],
                 lists:sort( merge(GC1, GC2)) ).

equal_test() ->
    GC1 = [{1, 2}, {2, 1}, {4, 1}],
    GC2 = [{1, 1}, {2, 4}, {3, 1}],
    GC3 = [{1, 2}, {2, 1}, {4, 1}],
    GC4 = [{4, 1}, {1, 2}, {2, 1}],
    ?assertNot(equal(GC1, GC2)),
    ?assert(equal(GC1, GC3)),
    ?assert(equal(GC1, GC4)).

usage_test() ->
    GC1 = new(),
    GC2 = new(),
    ?assert(equal(GC1, GC2)),
    GC1_1 = update({increment, 2}, a1, GC1),
    GC2_1 = update(increment, a2, GC2),
    GC3 = merge(GC1_1, GC2_1),
    GC2_2 = update({increment, 3}, a3, GC2_1),
    GC3_1 = update(increment, a4, GC3),
    GC3_2 = update(increment, a1, GC3_1),
    ?assertEqual([{a1, 3}, {a2, 1}, {a3, 3}, {a4, 1}],
                 lists:sort(merge(GC3_2, GC2_2))).

roundtrip_bin_test() ->
    GC = new(),
    GC1 = update({increment, 2}, <<"a1">>, GC),
    GC2 = update({increment, 4}, a2, GC1),
    GC3 = update(increment, "a4", GC2),
    GC4 = update({increment, 10000000000000000000000000000000000000000}, {complex, "actor", [<<"term">>, 2]}, GC3),
    Bin = to_binary(GC4),
    Decoded = from_binary(Bin),
    ?assert(equal(GC4, Decoded)).

lots_of_actors_test() ->
    GC = lists:foldl(fun(_, GCnt) ->
                            ActorLen = crypto:rand_uniform(1, 1000),
                            Actor = crypto:rand_bytes(ActorLen),
                            Cnt = crypto:rand_uniform(1, 10000),
                            riak_dt_gcounter:update({increment, Cnt}, Actor, GCnt) end,
                    new(),
                    lists:seq(1, 1000)),
    Bin = to_binary(GC),
    Decoded = from_binary(Bin),
    ?assert(equal(GC, Decoded)).

-endif.
