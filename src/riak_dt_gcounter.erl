%% -------------------------------------------------------------------
%%
%% riak_dt_gcounter: A state based, grow only, convergent counter
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

%%% @doc
%%% a G-Counter CRDT, borrows liberally from argv0 and Justin Sheehy's vclock module
%%% @end

-module(riak_dt_gcounter).

-behaviour(riak_dt).

-export([new/0, value/1, update/3, merge/2, equal/2]).
-export([gc_ready/2, gc_propose/2, gc_execute/2]).

-include("riak_dt_gc.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type gcounter() :: [{actor(), pos_integer()}].
-export_type([gcounter/0]).

%% EQC generator
-ifdef(EQC).
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

new() ->
    [].

value(GCnt) ->
    lists:sum([ Cnt || {_Act, Cnt} <- GCnt]).

update(increment, Actor, GCnt) ->
    increment_by(1, Actor, GCnt);
update({increment, Amount}, Actor, GCnt) when is_integer(Amount), Amount > 0 ->
    increment_by(Amount, Actor, GCnt).

merge(GCnt1, GCnt2) ->
    merge(GCnt1, GCnt2, []).

merge([], [], Acc) ->
    lists:reverse(Acc);
merge(LeftOver, [], Acc) ->
    lists:reverse(Acc, LeftOver);
merge([], LeftOver, Acc) ->
    lists:reverse(Acc, LeftOver);
merge([{Actor1, Cnt1}=AC1|Rest], Clock2, Acc) ->
    case lists:keytake(Actor1, 1, Clock2) of
        {value, {Actor1, Cnt2}, RestOfClock2} ->
            merge(Rest, RestOfClock2, [{Actor1, max(Cnt1, Cnt2)}|Acc]);
        false ->
            merge(Rest, Clock2, [AC1|Acc])
    end.

equal(VA,VB) ->
    lists:sort(VA) =:= lists:sort(VB).

%%% GC

-type gc_op() :: term().

% We're ready to GC if the actors we can't compact make up more than `compact_proportion` of
% the actors in this GCounter.
-spec gc_ready(gc_meta(), gcounter()) -> boolean().
gc_ready(Meta, GCnt) ->
    ROActors = length(ro_actors(Meta, GCnt)),
    TotalActors = length(GCnt),
    Proportion = Meta?GC_META.compact_proportion,
    TotalActors * Proportion > ROActors.


-spec gc_propose(gc_meta(), gcounter()) -> gc_op().
gc_propose(Meta, GCnt) ->
    Actor = ?GC_META_ACTOR(Meta),
    ROActors = ro_actors(Meta, GCnt),
    DeadActors = make_proposal(Actor, ROActors, GCnt),
    {?MODULE, Actor, DeadActors}.

-spec gc_execute(gc_op(), gcounter()) -> gcounter().
gc_execute({?MODULE, Actor, DeadActors}, GCnt0) ->
    GCnt1 = [Pair || {Act,_}=Pair <- GCnt0, not lists:member(Act, [A || {A,_} <- DeadActors])],
    ActorAdd = lists:foldl(fun({_,Cnt}, Sum) ->
                                Sum + Cnt
                           end, 0, DeadActors),
    increment_by(ActorAdd, Actor, GCnt1).

make_proposal(Actor, ROActors, GCnt) ->
    make_proposal(Actor, ROActors, GCnt, []).

make_proposal(_Actor, _ROActors, [], Dead) ->
    Dead;
make_proposal(Actor, ROActors, [{Act,_Cnt}|GCnt], Dead) when Actor == Act ->
    make_proposal(Actor, ROActors, GCnt, Dead);
make_proposal(Actor, ROActors, [{Act,Cnt}|GCnt], Dead) ->
    case ordsets:is_element(Act, ROActors) of
        true -> make_proposal(Actor, ROActors, GCnt, Dead);
        false -> make_proposal(Actor, ROActors, GCnt, [{Act,Cnt}|Dead])
    end.
    
% Returns the actors from GCnt that we can't get rid of during compaction.
ro_actors(Meta, GCnt) ->
    PAs = ordsets:from_list(Meta?GC_META.primary_actors),
    RoAs = ordsets:from_list(Meta?GC_META.readonly_actors),
    CounterActors = ordsets:from_list([Actor || {Actor, _} <- GCnt]),
    ordsets:intersection(ordsets:union(PAs, RoAs), CounterActors).


%% priv
increment_by(Amount, Actor, GCnt) when is_integer(Amount), Amount > 0 ->
    {Ctr, NewGCnt} = case lists:keytake(Actor, 1, GCnt) of
                         false ->
                             {Amount, GCnt};
                         {value, {_N, C}, ModGCnt} ->
                             {C + Amount, ModGCnt}
                     end,
    [{Actor, Ctr}|NewGCnt].


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(0, 1000, ?MODULE))]}.
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


-endif.
