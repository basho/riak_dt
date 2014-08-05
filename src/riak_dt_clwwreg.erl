%% -------------------------------------------------------------------
%%
%% riak_dt_clwwreg: A Causal last-write-wins register
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
%% A Causal LWW Register CRDT.
%%
%% This register tries to use causality tracking to resolve update
%% ordering as much as possible. When this breaks down because of
%% causally-concurrent updates, it uses LWW to resolve which update
%% to take the value of.
%%
%% This works much in the same way that riak_object works with
%% allow_mult=false
%%
%% @end

-module(riak_dt_clwwreg).
-behaviour(riak_dt).

-export([new/0, value/1, value/2, update/3, update/4, merge/2,
         equal/2, to_binary/1, from_binary/1, stats/1, stat/2,
         precondition_context/1, parent_clock/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, gen_op/1, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-define(NUMTESTS, 1000).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([clwwreg/0, clwwreg_op/0]).

-type clwwreg() :: dvvset:clock().
-type clwwreg_op() :: {assign, term(), timestamp()} | {assign, term()}.

-type entry() :: {term(), timestamp()}.
-type timestamp() :: non_neg_integer().

-spec new() -> clwwreg().
new() ->
    dvvset:new_list([]).

-spec value(clwwreg()) -> term().
value(ClwwReg) ->
    case get_lww_pair(ClwwReg) of
        {Val, _Ts} -> Val;
        _ -> undefined
    end.
                         

-spec get_lww_pair(dvvset:clock()) -> entry() | undefined.
get_lww_pair(DVVSet) ->
    case dvvset:size(DVVSet) of
        0 -> undefined;
        _ -> LwwDVV = dvvset:lww(fun lww/2, DVVSet),
             [Pair | _] = dvvset:values(LwwDVV),
             Pair
    end.

-spec lww(entry(), entry()) -> boolean().
lww({_,TsA},{_,TsB}) ->
    TsA =< TsB.

-spec value(term(), clwwreg()) -> term().
value(all_values, ClwwReg) ->
    dvvset:values(ClwwReg);
value(timestamp, ClwwReg) ->
    case get_lww_pair(ClwwReg) of
        undefined -> undefined;
        {_Val, Ts} -> Ts
    end;
value(_, V) ->
    value(V).

-spec update(clwwreg_op(), term(), clwwreg()) -> {ok, clwwreg()}.
update({assign, Val, Ts}, Actor, ClwwReg) ->
    NewDVVSet = dvvset:new({Val, Ts}),
    ClwwReg1 = dvvset:update(NewDVVSet, ClwwReg, Actor),
    {ok, ClwwReg1};
update({assign, Val}, Actor, ClwwReg) ->
    Ts = make_micro_epoch(),
    update({assign, Val, Ts}, Actor, ClwwReg).

-spec update(clwwreg_op(), term(), clwwreg(), riak_dt:context()) -> {ok, clwwreg()}.
update({assign, Val, Ts}, Actor, ClwwReg, Ctx) ->
    NewDVVSet = dvvset:new(Ctx, {Val, Ts}),
    DVVSet = dvvset:update(NewDVVSet, ClwwReg, Actor),
    {ok, DVVSet};
update({assign, Val}, Actor, ClwwReg, Ctx) ->
    Ts = make_micro_epoch(),
    update({assign, Val, Ts}, Actor, ClwwReg, Ctx).

make_micro_epoch() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

-spec parent_clock(riak_dt_vclock:vclock(), clwwreg()) -> clwwreg().
parent_clock(_Clock, ClwwReg) ->
    ClwwReg.

-spec merge(clwwreg(), clwwreg()) -> clwwreg().
merge(ClwwRegA, ClwwRegB) ->
    dvvset:sync([ClwwRegA, ClwwRegB]).

-spec equal(clwwreg(), clwwreg()) -> boolean().
equal(ClwwRegA, ClwwRegB) ->
    dvvset:equal(ClwwRegA, ClwwRegB) andalso
        equal_values(ClwwRegA, ClwwRegB).

equal_values(ClwwRegA, ClwwRegB) ->
    lists:sort(value(all_values, ClwwRegA)) =:=
        lists:sort(value(all_values, ClwwRegB)).

-spec precondition_context(clwwreg()) -> term().
precondition_context(ClwwReg) ->
    dvvset:join(ClwwReg).

-spec stats(clwwreg()) -> [{atom(), number()}].
stats(ClwwReg) ->
    [
     {S, stat(S, ClwwReg)} ||
        S <- [actor_count, value_count]
    ].

-spec stat(atom(), clwwreg()) -> number() | undefined.
stat(actor_count, ClwwReg) ->
    length(dvvset:ids(ClwwReg));
stat(value_count, ClwwReg) ->
    dvvset:size(ClwwReg);
stat(_,_) ->
    undefined.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_CLWWREG_TAG).
-define(V1_VERS, 1).

-spec to_binary(clwwreg()) -> binary().
to_binary(ClwwReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(ClwwReg))/binary>>.

-spec from_binary(binary()) -> clwwreg().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    riak_dt:from_binary(Bin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

%% Really not sure how to get this to work, so leaving it for the moment.
%% There's a test in riak_kv/tests which makes sure riak_object works like
%% dvvset does, this module just directly calls dvvset, so I see little point
%% in making a test like that.
%%
%eqc_value_test_() ->
%    crdt_statem_eqc:run(?MODULE, ?NUMTESTS).

bin_roundtrip_test_() ->
    crdt_statem_eqc:run_binary_rt(?MODULE, ?NUMTESTS).

generate() ->
    ?LET({Op, Actor}, {gen_op(), char()},
         begin
             {ok, Clww} = riak_dt_clwwreg:update(Op, Actor, riak_dt_clwwreg:new()),
             Clww
         end).

init_state() ->
    undefined.

gen_op(_Size) ->
    gen_op().

gen_op() ->
    ?LET(TS, largeint(), {assign, binary(), abs(TS)}).

update_expected(_ID, {assign, _Val, _Ts}, Wat) ->
    % FIXME
    Wat.

eqc_state_value(Wat) ->
    Wat.

-endif.

new_test() ->
    A = new(),
    ?assertEqual(undefined, value(A)),
    ?assertEqual(undefined, value(timestamp, A)),
    ?assertEqual([], value(all_values, A)).

single_assign_test() ->
    A = new(),
    {ok, A1} = update({assign, test, 1}, 'a', A),
    ?assertEqual(test, value(A1)),
    ?assertEqual(1, value(timestamp, A1)),
    ?assertEqual([{test, 1}], value(all_values, A1)).

lww_test() ->
    A = new(),
    {ok, B} = update({assign, test1, 1}, 'b', A),
    {ok, C} = update({assign, test2, 2}, 'c', A),
    A1 = merge(B,C),
    ?assertEqual(test2, value(A1)),
    ?assertEqual([{test1,1},{test2,2}], value(all_values, A1)).

%% two assignments, both onto a, but the second uses the Ctx returned
%% from the first, so must be causally "after". This then only uses
%% causality to resolve the update, it ignores the timestamps (which
%% appear to go backwards).
causal_beats_lww_test() ->
    A = new(),
    {ok, B} = update({assign, test2, 2}, 'b', A),
    Ctx = precondition_context(B),
    {ok, C} = update({assign, test1, 1}, 'c', A, Ctx),
    A1 = merge(B,C),
    ?assertEqual(test1, value(A1)),
    ?assertEqual([{test1,1}], value(all_values, A1)).

%% This shows that if you only have a context for a subset of updates
%% (in this case B and C), then you can't supersede the updates you haven't
%% seen (in this case D), so its values stick around, even if not exposed
%% due to lww.
causal_beats_some_test() ->
    A = new(),
    {ok, B} = update({assign, test1, 1}, 'b', A),
    {ok, C} = update({assign, test2, 2}, 'c', A),
    {ok, D} = update({assign, test3, 3}, 'd', A),
    BCCtx = precondition_context(merge(B,C)),
    {ok, A1} = update({assign, test4, 4}, 'b', B, BCCtx),
    A2 = merge(A1,D),
    ?assertEqual(test4, value(A2)),
    ?assertEqual([{test4,4},{test3,3}], value(all_values, A2)).

%% This test just shows that even if both updates happen at 'a', if one doesn't
%% have the right causal context, then they're stil concurrent.
context_respected_even_with_actor_test() ->
    A0 = new(),
    {ok, A1} = update({assign, test1, 1}, 'a', A0),
    {ok, A2} = update({assign, test2, 2}, 'a', A1, precondition_context(A0)),
    ?assertEqual(test2, value(A2)),
    ?assertEqual([{test2,2},{test1,1}], value(all_values, A2)).
    

-endif.

