%% -------------------------------------------------------------------
%%
%% riak_dt_pncounter: A convergent, replicated, state based PN counter
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
%% A PN-Counter CRDT. A PN-Counter is essentially two G-Counters: one for increments and
%% one for decrements. The value of the counter is the difference between the value of the
%% Positive G-Counter and the value of the Negative G-Counter.
%%
%% @see riak_kv_gcounter.erl
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(riak_dt_pncounter).

-behaviour(riak_dt).
-behaviour(riak_dt_gc).

-export([new/0, new/2, value/1, value/2, update/3, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).
-export([to_binary/2, from_binary/2, current_version/1, change_versions/3]).
-export([gc_epoch/1, gc_ready/2, gc_get_fragment/2, gc_replace_fragment/3]).

-include("riak_dt_gc_meta.hrl").

-export_type([pncounter/0, pncounter_op/0]).

-opaque pncounter()  :: [{Actor::riak_dt:actor(), Inc::pos_integer(), Dec::pos_integer()}].
-type pncounter_op() :: riak_dt_gcounter:gcounter_op() | decrement_op().
-type decrement_op() :: decrement | {decrement, pos_integer()}.
-type pncounter_q()  :: positive | negative.

-type v1_pncounter() :: {riak_dt_gcounter:gcounter(),riak_dt_gcounter:gcounter()}.
-type version()      :: integer().
-type any_pncounter() :: pncounter() | v1_pncounter().

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-compile(export_all).

-behaviour(crdt_gc_statem_eqc).
-export([gen_gc_ops/0]).
-export([gc_model_create/0, gc_model_update/3, gc_model_merge/2, gc_model_realise/1]).
-export([gc_model_ready/2]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% @doc Create a new, empty `pncounter()'
-spec new() -> pncounter().
new() ->
    [].

%% @doc Create a `pncounter()' with an initial `Value' for `Actor'.
-spec new(term(), integer()) -> pncounter().
new(Actor, Value) when Value > 0 ->
    update({increment, Value}, Actor, new());
new(Actor, Value) when Value < 0 ->
    update({decrement, Value * -1}, Actor, new());
new(_Actor, _Zero) ->
    new().

%% @doc The single, total value of a `pncounter()'
-spec value(pncounter()) -> integer().
value(PNCnt) ->
    lists:sum([Inc - Dec || {_Act,Inc,Dec} <- PNCnt]).

%% @doc query the parts of a `pncounter()'
%% valid queries are `positive' or `negative'.
-spec value(pncounter_q(), pncounter()) -> integer().
value(positive, PNCnt) ->
    lists:sum([Inc || {_Act,Inc,_Dec} <- PNCnt]);
value(negative, PNCnt) ->
    lists:sum([Dec || {_Act,_Inc,Dec} <- PNCnt]).

%% @doc Update a `pncounter()'. The first argument is either the atom
%% `increment' or `decrement' or the two tuples `{increment, pos_integer()}' or
%% `{decrement, pos_integer()}'. In the case of the former, the operation's amount
%% is `1'. Otherwise it is the value provided in the tuple's second element.
%% `Actor' is any term, and the 3rd argument is the `pncounter()' to update.
%%
%% returns the updated `pncounter()'
-spec update(pncounter_op(), term(), pncounter()) -> pncounter().
update(increment, Actor, PNCnt) ->
    update({increment, 1}, Actor, PNCnt);
update(decrement, Actor, PNCnt) ->
    update({decrement, 1}, Actor, PNCnt);
update({_IncrDecr, 0}, _Actor, PNCnt) ->
    PNCnt;
update({increment, By}, Actor, PNCnt) when is_integer(By), By > 0 ->
    increment_by(By, Actor, PNCnt);
update({increment, By}, Actor, PNCnt) when is_integer(By), By < 0 ->
    update({decrement, -By}, Actor, PNCnt);
update({decrement, By}, Actor, PNCnt) when is_integer(By), By > 0 ->
    decrement_by(By, Actor, PNCnt).

%% @doc Merge two `pncounter()'s to a single `pncounter()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(pncounter(), pncounter()) -> pncounter().
merge(PNCntA, PNCntB) ->
    merge(PNCntA, PNCntB, []).

merge([],[],Acc) ->
    lists:reverse(Acc);
merge(LeftOver, [], Acc) ->
    lists:reverse(Acc,LeftOver);
merge([], RightOver, Acc) ->
    lists:reverse(Acc,RightOver);
merge([{Act,IncA,DecA}=ACntA|RestA],PNCntB, Acc) ->
    case lists:keytake(Act, 1, PNCntB) of
        {value, {Act,IncB,DecB}, ModPNCntB} ->
            ACntB = {Act,max(IncA,IncB),max(DecA,DecB)},
            merge(RestA, ModPNCntB, [ACntB|Acc]);
        false ->
            merge(RestA, PNCntB, [ACntA|Acc])
    end.

%% @doc Are two `pncounter()'s structurally equal? This is not `value/1' equality.
%% Two counters might represent the total `-42', and not be `equal/2'. Equality here is
%% that both counters represent exactly the same information.
-spec equal(pncounter(), pncounter()) -> boolean().
equal(PNCntA, PNCntB) ->
    lists:sort(PNCntA) =:= lists:sort(PNCntB).

-define(TAG, 71).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

%% @doc Encode an effecient binary representation of `pncounter()'
-spec to_binary(any_pncounter()) -> binary().
to_binary(PNCnt) ->
    to_binary(?V2_VERS, PNCnt).

%% @doc Decode a binary encoded PN-Counter
-spec from_binary(binary()) -> pncounter().
from_binary(Binary = <<?TAG:8/integer, _/binary>>) ->
    from_binary(?V2_VERS, Binary).


-spec to_binary(version(), any_pncounter()) -> binary().
to_binary(?V2_VERS, PNCnt) ->
    Version = current_version(PNCnt),
    V2 = change_versions(Version, ?V2_VERS, PNCnt),
    V2Bin = term_to_binary(V2),
    <<?TAG:8/integer, ?V2_VERS:8/integer, V2Bin/binary>>;
to_binary(?V1_VERS, PNCnt) ->
    Version = current_version(PNCnt),
    {P,N} = change_versions(Version, ?V1_VERS, PNCnt),
    PBin = riak_dt_gcounter:to_binary(P),
    NBin = riak_dt_gcounter:to_binary(N),
    PBinLen = byte_size(PBin),
    NBinLen = byte_size(NBin),
    <<?TAG:8/integer, ?V1_VERS:8/integer,
      PBinLen:32/integer, PBin:PBinLen/binary,
      NBinLen:32/integer, NBin:NBinLen/binary>>.

-spec from_binary(version(), binary()) -> any_pncounter().
from_binary(?V2_VERS, <<?TAG:8/integer, ?V2_VERS:8/integer, PNBin/binary>>) ->
    binary_to_term(PNBin);
from_binary(?V1_VERS, <<?TAG:8/integer, ?V1_VERS:8/integer,
                  PBinLen:32/integer, PBin:PBinLen/binary,
                  NBinLen:32/integer, NBin:NBinLen/binary>>) ->
    OldStyleIncs = riak_dt_gcounter:from_binary(PBin),
    OldStyleDecs = riak_dt_gcounter:from_binary(NBin),
    {OldStyleIncs,OldStyleDecs};
from_binary(OutVers, Binary = <<?TAG:8/integer, InVers:8/integer, _/binary>>) ->
    OtherVers = from_binary(InVers, Binary),
    change_versions(InVers, OutVers, OtherVers).

-spec current_version(any_pncounter()) -> version().
current_version(PNCnt) when is_list(PNCnt) ->
    ?V2_VERS;
current_version({_P,_N}) ->
    ?V1_VERS.

-spec change_versions(version(), version(), any_pncounter()) -> any_pncounter().
change_versions(Version, Version, PNCnt)  ->
    PNCnt;
change_versions(?V1_VERS, ?V2_VERS, {P,N}) ->
    PNCnt0 = new(),
    PNCnt1 = lists:foldl(fun({Actor,Inc},PNCnt) ->
                            update({increment,Inc}, Actor, PNCnt)
                         end, PNCnt0, P),
    PNCnt2 = lists:foldl(fun({Actor,Dec},PNCnt) ->
                            update({decrement,Dec}, Actor, PNCnt)
                         end, PNCnt1, N),
    PNCnt2;
change_versions(?V2_VERS, ?V1_VERS, PNCnt) when is_list(PNCnt) ->
    OldPN = {riak_dt_gcounter:new(), riak_dt_gcounter:new()},
    {P, N} = lists:foldl(fun({Actor,Inc,Dec},{P1,N1}) ->
                             {riak_dt_gcounter:update({increment,Inc},Actor,P1),
                              riak_dt_gcounter:update({increment,Dec},Actor,N1)}
                         end, OldPN, PNCnt),
    {P, N}.

% Priv
-spec increment_by(pos_integer(), term(), pncounter()) -> pncounter().
increment_by(Increment, Actor, PNCnt) ->
    case lists:keytake(Actor, 1, PNCnt) of
        false ->
            [{Actor,Increment,0}|PNCnt];
        {value, {Actor,Inc,Dec}, ModPNCnt} ->
            [{Actor,Inc+Increment,Dec}|ModPNCnt]
    end.

-spec decrement_by(pos_integer(), term(), pncounter()) -> pncounter().
decrement_by(Decrement, Actor, PNCnt) ->
    case lists:keytake(Actor, 1, PNCnt) of
        false ->
            [{Actor,0,Decrement}|PNCnt];
        {value, {Actor,Inc,Dec}, ModPNCnt} ->
            [{Actor,Inc,Dec+Decrement}|ModPNCnt]
    end.

%%% GC

-type gc_fragment() :: pncounter().

% We're ready to GC if either of the gcounters are ready to GC.
-spec gc_ready(gc_meta(), pncounter()) -> boolean().
gc_ready(Meta, PNCnt) ->
    GCActors = length([Act || {{gc, _Epoch}=Act,_Inc,_Dec} <- PNCnt]),
    ROActors = length(ro_actors(Meta, PNCnt)),
    TotalActors = length(PNCnt),
    case TotalActors of
        0 -> false;
        1 -> (GCActors > 1) or ?SHOULD_GC(Meta, ROActors/TotalActors)
    end.

-spec gc_epoch(pncounter()) -> riak_dt_gc:epoch().
gc_epoch(PNCnt) ->
    GCActors = [Act || {{gc, _Epoch}=Act,_Inc,_Dec} <- PNCnt],
    {gc, Epoch} = hd(GCActors),
    Epoch.

-spec gc_get_fragment(gc_meta(), pncounter()) -> gc_fragment().
gc_get_fragment(Meta, PNCnt) ->
    ROActors = ro_actors(Meta, PNCnt),
    DeadActors = make_fragment(ROActors, PNCnt, []),
    DeadActors.

-spec gc_replace_fragment(gc_meta(), gc_fragment(), pncounter()) -> pncounter().
gc_replace_fragment(Meta, Frag, PNCnt) ->
    Epoch = Meta?GC_META.epoch,
    {AInc, ADec, PNCnt1} = subtract_dead(Frag, PNCnt, {0,0}),
    PNCnt2 = prune_empty_nodes(PNCnt1),
    [{{gc, Epoch},AInc,ADec} | PNCnt2].

ro_actors(Meta, PNCnt) ->
    PAs = ordsets:from_list(Meta?GC_META.primary_actors),
    RoAs = ordsets:from_list(Meta?GC_META.readonly_actors),
    CounterActors = ordsets:from_list([Actor || {Actor, _, _} <- PNCnt]),
    ordsets:intersection(ordsets:union(PAs, RoAs), CounterActors).

make_fragment(_ROActors, [], Dead) ->
    Dead;
% Ensure previous GCs are all removed.
make_fragment(ROActors, [{{gc, _Epoch}=Act,Inc,Dec}|PNCnt], Dead) ->
    make_fragment(ROActors, PNCnt, [{Act,Inc,Dec}|Dead]);
% And ensure any non-read-only actors are removed too
make_fragment(ROActors, [{Act,Inc,Dec}|PNCnt], Dead) ->
    case ordsets:is_element(Act, ROActors) of
        true ->  make_fragment(ROActors, PNCnt, Dead);
        false -> make_fragment(ROActors, PNCnt, [{Act,Inc,Dec}|Dead])
    end.

subtract_dead([], PNCnt, {Inc,Dec}) ->
    {Inc, Dec, PNCnt};
subtract_dead([{Actor,SubInc,SubDec}|Dead], PNCnt, {Inc,Dec}) ->
    {AInc1, ADec1, NewPNCnt} = case lists:keytake(Actor, 1, PNCnt) of
                                    false ->
                                        {-SubInc, -SubDec, PNCnt};
                                    {value, {_,AInc,ADec}, ModPNCnt} ->
                                        {AInc-SubInc, ADec-SubDec, ModPNCnt}
                               end,
    subtract_dead(Dead, [{Actor,AInc1,ADec1}|NewPNCnt], {Inc-SubInc,Dec-SubDec}).

prune_empty_nodes(PNCnt) ->
    [Pair || {_,Inc,Dec}=Pair <- PNCnt, Inc =/= 0, Dec =/= 0].

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
%% EQC generator
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

eqc_gc_test_() ->
    crdt_statem_eqc:run(?MODULE, 200).

generate() ->
    ?LET(Ops, list(gen_op()),
         lists:foldl(fun(Op, Cntr) ->
                             riak_dt_pncounter:update(Op, choose(1, 50), Cntr) end,
                     riak_dt_pncounter:new(),
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

gen_gc_ops() ->
    [].

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

% In my gc model, I'm thinking of it as a log of update operations (each with a unique ID).
gc_model_create() ->
    ordsets:new().

gc_model_update(increment, Actor, Cnt) ->
    add_change(1, Actor, Cnt);
gc_model_update({increment, Add}, Actor, Cnt) ->
    add_change(Add, Actor, Cnt);
gc_model_update(decrement, Actor, Cnt) ->
    add_change(-1, Actor, Cnt);
gc_model_update({decrement, Dec}, Actor, Cnt) ->
    add_change(-abs(Dec), Actor, Cnt).

add_change(Change, Actor, Cnt) ->
    ID = erlang:phash2({Actor, erlang:now()}),
    ordsets:add_element({ID, Change}, Cnt).

gc_model_merge(Cnt1, Cnt2) ->
    ordsets:union(Cnt1, Cnt2).

gc_model_realise(Cnt) ->
    ordsets:fold(fun ({_ID,Add},Sum) ->
            Add + Sum
        end, 0, Cnt).

% TODO: this should fail more (or at all)
gc_model_ready(_Meta, _Cnt) ->
    false.
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
    PNCnt1 = update(increment, 1, PNCnt0),
    PNCnt2 = update(increment, 2, PNCnt1),
    PNCnt3 = update(increment, 1, PNCnt2),
    ?assertEqual([{1,2,0}, {2,1,0}], PNCnt3).

update_increment_by_test() ->
    PNCnt0 = new(),
    PNCnt1 = update({increment, 7}, 1, PNCnt0),
    ?assertEqual([{1,7,0}], PNCnt1).

update_decrement_test() ->
    PNCnt0 = new(),
    PNCnt1 = update(increment, 1, PNCnt0),
    PNCnt2 = update(increment, 2, PNCnt1),
    PNCnt3 = update(increment, 1, PNCnt2),
    PNCnt4 = update(decrement, 1, PNCnt3),
    ?assertEqual([{1,2,1}, {2,1,0}], PNCnt4).

update_decrement_by_test() ->
    PNCnt0 = new(),
    PNCnt1 = update({increment, 7}, 1, PNCnt0),
    PNCnt2 = update({decrement, 5}, 1, PNCnt1),
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
    PNCnt1_1 = update({increment, 2}, a1, PNCnt1),
    PNCnt2_1 = update(increment, a2, PNCnt2),
    PNCnt3 = merge(PNCnt1_1, PNCnt2_1),
    PNCnt2_2 = update({increment, 3}, a3, PNCnt2_1),
    PNCnt3_1 = update(increment, a4, PNCnt3),
    PNCnt3_2 = update(increment, a1, PNCnt3_1),
    PNCnt3_3 = update({decrement, 2}, a5, PNCnt3_2),
    PNCnt2_3 = update(decrement, a2, PNCnt2_2),
    ?assertEqual([{a5,0,2},
                  {a1,3,0},
                  {a4,1,0},
                  {a2,1,1},
                  {a3,3,0}], merge(PNCnt3_3, PNCnt2_3)).

roundtrip_bin_test() ->
    PN = new(),
    PN1 = update({increment, 2}, <<"a1">>, PN),
    PN2 = update({decrement, 1000000000000000000000000}, douglas_Actor, PN1),
    PN3 = update(increment, [{very, ["Complex"], <<"actor">>}, honest], PN2),
    PN4 = update(decrement, "another_acotr", PN3),
    Bin = to_binary(PN4),
    Decoded = from_binary(Bin),
    ?assert(equal(PN4, Decoded)).

% This is kinda important, I should probably do more about this test.
update_bin_test() ->
    OldPNCnt = {[{a,1}],[{b,3}]},
    OldPNBin = to_binary(?V1_VERS, OldPNCnt),
    NewPNCnt = from_binary(OldPNBin),
    ?assertEqual([{b,0,3},{a,1,0}], NewPNCnt).

query_test() ->
    PN = new(),
    PN1 = update({increment, 50}, a1, PN),
    PN2 = update({increment, 50}, a2, PN1),
    PN3 = update({decrement, 15}, a3, PN2),
    PN4 = update({decrement, 10}, a4, PN3),
    ?assertEqual(75, value(PN4)),
    ?assertEqual(100, value(positive, PN4)),
    ?assertEqual(25, value(negative, PN4)).

-endif.
