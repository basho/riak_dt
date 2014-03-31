%% -------------------------------------------------------------------
%%
%% riak_dt_emcntr: A convergent, replicated, state based PN counter,
%% for embedding in riak_dt_map.
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

%% @doc A PN-Counter CRDT. A PN-Counter is essentially two G-Counters:
%% one for increments and one for decrements. The value of the counter
%% is the difference between the value of the Positive G-Counter and
%% the value of the Negative G-Counter. However, this PN-Counter is
%% for using embedded in a riak_dt_map. The problem with an embedded
%% pn-counter is when the field is removed and added again. PN-Counter
%% merge takes the max of P and N as the merged value. In the case
%% that a field was removed and re-added P and N maybe be _lower_ than
%% their removed values, and when merged with a replica that has not
%% seen the remove, the remove is lost. This counter adds some
%% causality by storing a `dot` with P and N. Merge takes the max
%% event for each actor, so newer values win over old ones. The rest
%% of the mechanics are the same.
%%
%% @see riak_kv_gcounter.erl
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(riak_dt_emcntr).
-behaviour(riak_dt).

-export([new/0, value/1, value/2]).
-export([update/3, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).
-export([stats/1, stat/2]).
-export([parent_clock/2, update/4]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type emcntr() :: {riak_dt_vclock:vclock(), [entry()]}.
-type entry() :: {Actor :: riak_dt:actor(), {Event :: pos_integer(),
                                             Inc :: pos_integer(),
                                             Dec :: pos_integer()}
                 }.

-spec new() -> emcntr().
new() ->
    {riak_dt_vclock:fresh(), orddict:new()}.

parent_clock(Clock, {_, Cntr}) ->
    {Clock, Cntr}.

value({_Clock, PNCnt}) ->
    lists:sum([Inc - Dec || {_Act, {_Event, Inc, Dec}} <- PNCnt]).

value(positive, {_Clock, PNCnt}) ->
    lists:sum([Inc || {_Act, {_Evt, Inc, _Dec}} <- PNCnt]);
value(negative, {_Clock, PNCnt}) ->
    lists:sum([Dec || {_Act, {_Evt, _Inc, Dec}} <- PNCnt]).

update(Op, {Actor, Cnt}=Dot, {Clock, PNCnt}) when is_tuple(Dot) ->
    Clock2 = riak_dt_vclock:merge([[Dot], Clock]),
    Entry = orddict:find(Actor, PNCnt),
    {Inc, Dec} = op(Op, Entry),
    {ok, {Clock2, orddict:store(Actor, {Cnt, Inc, Dec}, PNCnt)}}.

update(Op, Actor, Cntr, _Ctx) ->
    update(Op, Actor, Cntr).

op(Op, error) ->
    op(Op, {0, 0});
op(Op, {ok, {_Evt, P, N}}) ->
    op(Op, {P, N});
op(increment, {P, N}) ->
    {P+1, N};
op(decrement, {P, N}) ->
    {P, N+1};
op({increment, By}, {P, N}) ->
    {P+By, N};
op({decrement, By}, {P, N}) ->
    {P, N+By}.

merge(Cnt, Cnt) ->
    Cnt;
merge({ClockA, CntA}, {ClockB, CntB}) ->
    Clock = riak_dt_vclock:merge([ClockA, ClockB]),
    {Cnt0, BUnique} = merge_left(ClockB, CntA, CntB),
    Cnt = merge_right(ClockA, BUnique, Cnt0),
    {Clock, Cnt}.

merge_left(ClockB, A, B) ->
    orddict:fold(fun(Actor, {Evt, _Inc, _Dec}=Cnt, {Keep, BUnique}) ->
                         case orddict:find(Actor, B) of
                             error ->
                                 case riak_dt_vclock:descends(ClockB, [{Actor, Evt}]) of
                                     true ->
                                         {Keep, BUnique};
                                     false ->
                                         {orddict:store(Actor, Cnt, Keep), BUnique}
                                 end;
                             {ok, {E2, I, D}} when E2 > Evt ->
                                 {orddict:store(Actor, {E2, I, D}, Keep), orddict:erase(Actor, BUnique)};
                             {ok, _} ->
                                 {orddict:store(Actor, Cnt, Keep), orddict:erase(Actor, BUnique)}
                         end
                 end,
                 {orddict:new(), B},
                 A).

merge_right(ClockA, BUnique, Acc) ->
    orddict:fold(fun(Actor, {Evt, _Inc, _Dec}=Cnt, Keep) ->
                         case riak_dt_vclock:descends(ClockA, [{Actor, Evt}]) of
                             true ->
                                 Keep;
                             false ->
                                 orddict:store(Actor, Cnt, Keep)
                         end
                 end,
                 Acc,
                 BUnique).

equal({ClockA, PNCntA}, {ClockB, PNCntB}) ->
    riak_dt_vclock:equal(ClockA, ClockB) andalso
        PNCntA =:= PNCntB.

stats(Emcntr) ->
    [{actor_count, stat(actor_count, Emcntr)}].

stat(actor_count, {Clock, _Emcntr}) ->
    length(Clock);
stat(_, _) -> undefined.


-include("riak_dt_tags.hrl").
-define(TAG, 900).%%?DT_EMCNTR_TAG).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

to_binary(PNCnt) ->
    <<?TAG:8/integer, (term_to_binary(PNCnt))/binary>>.

%% @doc Decode a binary encoded PN-Counter
from_binary(<<?TAG:8/integer, Bin/binary>>) ->
    binary_to_term(Bin).

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
                             {ok, Cntr2} = riak_dt_emcntr:update(Op, choose(1, 50), Cntr),
                             Cntr2
                     end,
                     riak_dt_emcntr:new(),
                     Ops)).

init_state() ->
    0.

gen_op() ->
    oneof([increment,
           {increment, nat()},
           decrement,
           {decrement, nat()},
           {increment, ?LET(X, nat(), -X)}
          ]).

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

new_test() ->
    ?assertEqual([], new()).

value_test() ->
    PNCnt1 = [{1,1,0}, {2,13,10}, {3,1,0}, {4,0,1}],
    PNCnt2 = [],
    PNCnt3 = [{1,3,3},{2,1,1},{3,1,1}],
    ?assertEqual(4, value(PNCnt1)),
    ?assertEqual(0, value(PNCnt2)),
    ?assertEqual(0, value(PNCnt3)).

update_increment_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update(increment, 1, PNCnt0),
    {ok, PNCnt2} = update(increment, 2, PNCnt1),
    {ok, PNCnt3} = update(increment, 1, PNCnt2),
    ?assertEqual([{1,2,0}, {2,1,0}], PNCnt3).

update_increment_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 7}, 1, PNCnt0),
    ?assertEqual([{1,7,0}], PNCnt1).

update_decrement_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update(increment, 1, PNCnt0),
    {ok, PNCnt2} = update(increment, 2, PNCnt1),
    {ok, PNCnt3} = update(increment, 1, PNCnt2),
    {ok, PNCnt4} = update(decrement, 1, PNCnt3),
    ?assertEqual([{1,2,1}, {2,1,0}], PNCnt4).

update_decrement_by_test() ->
    PNCnt0 = new(),
    {ok, PNCnt1} = update({increment, 7}, 1, PNCnt0),
    {ok, PNCnt2} = update({decrement, 5}, 1, PNCnt1),
    ?assertEqual([{1,7,5}], PNCnt2).

merge_test() ->
    PNCnt1 = [{<<"1">>,1,0},
              {<<"2">>,2,0},
              {<<"4">>,4,0}],
    PNCnt2 = [{<<"3">>,3,0},
              {<<"4">>,3,0}],
    ?assertEqual([], merge(new(), new())),
    ?assertEqual([{<<"1">>,1,0},
                  {<<"2">>,2,0},
                  {<<"4">>,4,0},
                  {<<"3">>,3,0}], merge(PNCnt1, PNCnt2)).

merge_too_test() ->
    PNCnt1 = [{<<"5">>,5,0},
              {<<"7">>,0,4}],
    PNCnt2 = [{<<"5">>,0,2},
              {<<"6">>,6,0},
              {<<"7">>,7,0}],
    ?assertEqual([{<<"5">>,5,2},
                  {<<"7">>,7,4},
                  {<<"6">>,6,0}], merge(PNCnt1, PNCnt2)).

equal_test() ->
    PNCnt1 = [{1,2,1},{2,1,0},{3,0,1},{4,1,0}],
    PNCnt2 = [{1,1,0},{2,4,0},{3,1,0}],
    PNCnt3 = [{4,1,0},{2,1,0},{3,0,1},{1,2,1}],
    PNCnt4 = [{4,1,0},{1,2,1},{2,1,0},{3,0,1}],
    ?assertNot(equal(PNCnt1, PNCnt2)),
    ?assert(equal(PNCnt3, PNCnt4)),
    ?assert(equal(PNCnt1, PNCnt3)).

usage_test() ->
    PNCnt1 = new(),
    PNCnt2 = new(),
    ?assert(equal(PNCnt1, PNCnt2)),
    {ok, PNCnt1_1} = update({increment, 2}, a1, PNCnt1),
    {ok, PNCnt2_1} = update(increment, a2, PNCnt2),
    PNCnt3 = merge(PNCnt1_1, PNCnt2_1),
    {ok, PNCnt2_2} = update({increment, 3}, a3, PNCnt2_1),
    {ok, PNCnt3_1} = update(increment, a4, PNCnt3),
    {ok, PNCnt3_2} = update(increment, a1, PNCnt3_1),
    {ok, PNCnt3_3} = update({decrement, 2}, a5, PNCnt3_2),
    {ok, PNCnt2_3} = update(decrement, a2, PNCnt2_2),
    ?assertEqual([{a5,0,2},
                  {a1,3,0},
                  {a4,1,0},
                  {a2,1,1},
                  {a3,3,0}], merge(PNCnt3_3, PNCnt2_3)).

roundtrip_bin_test() ->
    PN = new(),
    {ok, PN1} = update({increment, 2}, <<"a1">>, PN),
    {ok, PN2} = update({decrement, 1000000000000000000000000}, douglas_Actor, PN1),
    {ok, PN3} = update(increment, [{very, ["Complex"], <<"actor">>}, honest], PN2),
    {ok, PN4} = update(decrement, "another_acotr", PN3),
    Bin = to_binary(PN4),
    Decoded = from_binary(Bin),
    ?assert(equal(PN4, Decoded)).

query_test() ->
    PN = new(),
    {ok, PN1} = update({increment, 50}, a1, PN),
    {ok, PN2} = update({increment, 50}, a2, PN1),
    {ok, PN3} = update({decrement, 15}, a3, PN2),
    {ok, PN4} = update({decrement, 10}, a4, PN3),
    ?assertEqual(75, value(PN4)),
    ?assertEqual(100, value(positive, PN4)),
    ?assertEqual(25, value(negative, PN4)).

stat_test() ->
    PN = new(),
    {ok, PN1} = update({increment, 50}, a1, PN),
    {ok, PN2} = update({increment, 50}, a2, PN1),
    {ok, PN3} = update({decrement, 15}, a3, PN2),
    {ok, PN4} = update({decrement, 10}, a4, PN3),
    ?assertEqual([{actor_count, 0}], stats(PN)),
    ?assertEqual(4, stat(actor_count, PN4)),
    ?assertEqual(undefined, stat(max_dot_length, PN4)).
-endif.
