%% -------------------------------------------------------------------
%%
%% riak_dt_zorset: Sorted set. Basically a cheat, uses a Map of element->counter
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

%% @doc A sorted set. Just a Map of element->counter.
%%
%% @see riak_dt_multi, riak_dt_vclock, riak_dt_vvorset
%%
%% @end
-module(riak_dt_zorset).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, reset/2]).
-export([update/3, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-export_type([zorset/0, zorset_op/0]).

-type zorset() :: riak_dt_multi:multi().
-type zorset_op() :: {add, member(), score()} | {remove, member()} | {incr, member(), score()}.

-type zorset_q()  :: size | {contains, term()}.

-type member() :: term().
-type score() :: integer().

-define(ELEM(Elem), {Elem, riak_dt_pncounter}).

-spec new() -> zorset().
new() ->
    riak_dt_multi:new().

-spec value(zorset()) -> [member()].
value(ZORset) ->
    Members = riak_dt_multi:value(ZORset),
    lists:keysort(1, [{Score, Name} || {{Name, _Type}, Score} <- Members]).

%% @doc Query `OR-Set`
-spec value(zorset_q(), zorset()) -> term().
value(size, ZORset) ->
    length(value(ZORset));
value({contains, Elem}, ZORset) ->
    lists:member(Elem, value(ZORset)).

-spec update(zorset_op(), term(), zorset()) -> zorset().
update({add, Elem, Score}, Actor, ZORSet) ->
    PNCounter = riak_dt_pncounter:new(Actor, Score),
    riak_dt_multi:update({update, [{insert, ?ELEM(Elem), PNCounter}]}, Actor, ZORSet);
update({remove, Elem}, Actor, ZORSet) ->
    riak_dt_multi:update({update, [{remove, ?ELEM(Elem)}]}, Actor, ZORSet);
update({incr, Elem, By}, Actor, ZORSet) when By /= 0 ->
    riak_dt_multi:update({update, [{update, ?ELEM(Elem), counter_op(By)}]}, Actor, ZORSet);
update({incr, _Elem, _By}, _Actor, ZORSet) ->
    ZORSet.

%% Create a counter op the pncounter understands
counter_op(Amt) when Amt < 0 ->
    {decrement, -1*Amt};
counter_op(Amt) when Amt > 0 ->
    {increment, Amt}.

-spec merge(zorset(), zorset()) -> zorset().
merge(ZORSet1, ZORSet2) ->
    riak_dt_multi:merge(ZORSet1, ZORSet2).

-spec equal(zorset(), zorset()) -> boolean().
equal(ZORSet1, ZORSet2) ->
    riak_dt_multi:equal(ZORSet1, ZORSet2).

%% @Doc reset to 'empty' by having
%% `Actor' remove all entries
-spec reset(zorset(), term()) -> zorset().
reset(ZSet, Actor) ->
    Members = value(ZSet),
    reset(Members, Actor, ZSet).

reset([], _Actor, ZSet) ->
    ZSet;
reset([{_Score, Name} | Rest], Actor, ZSet) ->
    reset(Rest, Actor, update({remove, Name}, Actor, ZSet)).

-define(TAG, 78).
-define(V1_VERS, 1).

-spec to_binary(zorset()) -> binary().
to_binary(ZORSet) ->
    MBin = riak_dt_multi:to_binary(ZORSet),
    <<?TAG:8/integer, ?V1_VERS:8/integer, MBin/binary>>.

-spec from_binary(binary()) -> zorset().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, MBin/binary>>) ->
    riak_dt_multi:from_binary(MBin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
generate() ->
    ?LET(Ops, list(gen_op()),
         lists:foldl(fun(Op, ZSet) ->
                             riak_dt_zorset:update(Op, choose(1, 50), ZSet) end,
                     new(),
                     Ops)).

gen_op() ->
    ?LET({Elem, Score}, {binary(8), int()},
         oneof([{add, Elem, Score}, {remove, Elem}, {incr, Elem, Score}])).

init_state() ->
    {0, dict:new()}.

update_expected(ID, {add, Elem, Score}, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    D2 = dict:update({Elem, cnt}, fun(V) -> Score + V end, Score, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, D2)};
update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
    D2 = dict:erase({Elem, cnt}, Dict),
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, D2)};
update_expected(_ID, {incr, _Elem, 0}, Prev) ->
    Prev;
update_expected(ID, {incr, Elem, Score}, {Cnt, Dict}) ->
    update_expected(ID, {add, Elem, Score}, {Cnt, Dict});
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)}.

eqc_state_value({_Cnt, Dict}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)};
                          ({_Elm, cnt},_V, Acc) -> Acc end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Values = lists:usort([ Elem || {Elem, _X} <- sets:to_list(Remaining)]),
    V2 = lists:foldl(fun(E, Acc) -> [{dict:fetch({E, cnt}, Dict), E} | Acc] end,
                [], Values),
    lists:keysort(1, V2).

-endif.

-endif.
