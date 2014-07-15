%% -------------------------------------------------------------------
%%
%% riak_dt_rangereg: A Register that stores the smallest,largest,first, and last integer assigned into it
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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
%% A register that keeps track of the largest, smallest, first, and last
%% integers that were ever assigned to it. All of these start as `undefined`,
%% but a single update can populate them all (as that integer is all of the
%% above), after which subsequent updates may or may not change the different
%% values depending on its value and timestamp:
%%
%% - the `max` field stores the largest integer, only being updated if
%%   the new integer is larger than its current value
%% - the `min` field stores the smallest integer, only being updated if
%%   the new integer is smaller than its current value
%% - the `first` field stores the first integer (by timestamp) and its
%%   timestamp, only updating if the new integer has a lower timestamp.
%%   If the timestamps are the same, the smaller of the two integers
%%   is kept.
%% - the `last` field stores the last integer (by timestamp) and its
%%   timestamp, only updating if the new integer has a higher timestamp.
%%   If the timestamps are the same, the larger of the two integers is
%%   kept.
%%
%% @end

-module(riak_dt_rangereg).
-behaviour(riak_dt).

-export([new/0, value/1, value/2, update/3, update/4, merge/2,
         equal/2, to_binary/1, from_binary/1, parent_clock/2,
         stats/1, stat/2]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([rangereg/0, rangereg_op/0]).

-record(rangereg, {
    max   :: rangereg_single(),
    min   :: rangereg_single(),
    first :: rangereg_pair(),
    last  :: rangereg_pair()
  }).

-opaque rangereg() :: #rangereg{}.
-type rangereg_pair() :: undefined | {integer(), integer()}.
-type rangereg_single() :: undefined | integer().
-type rangereg_op() :: {assign, integer(), integer()}.

-type nonintegral_error() :: {error, {type, {non_integral, term()}}}.

%% @doc Create a new, empty `rangereg()'
-spec new() -> rangereg().
new() ->
    #rangereg{}.

%% @doc The value of a `rangereg()': a proplist with 4 keys, max, min, first, last
-spec value(rangereg()) -> term().
value(#rangereg{max=Max, min=Min, first=First, last=Last}) ->
    [
      {max, Max}, 
      {min, Min}, 
      {first, pair_val(First)}, 
      {last, pair_val(Last)}
    ].

%% @private
pair_val(undefined) ->
  undefined;
pair_val({Val, _Ts}) ->
  Val.

%% @doc query this `rangereg()'.
-spec value(term(), rangereg()) -> term().
value(max, #rangereg{max=Max}) ->
  Max;
value(min, #rangereg{min=Min}) ->
  Min;
value(first, #rangereg{first=First}) ->
  pair_val(First);
value(first_ts, #rangereg{first=First}) ->
  pair_ts(First);
value(last, #rangereg{last=Last}) ->
  pair_val(Last);
value(last_ts, #rangereg{last=Last}) ->
  pair_ts(Last);
value(timerange, #rangereg{first=First,last=Last}) ->
  {range, pair_ts(First), pair_ts(Last)};
value(_, V) ->
    value(V).

%% @private
pair_ts(undefined) ->
  undefined;
pair_ts({_Val, Ts}) ->
  Ts.

%% @doc Assign a `Value' to the `rangereg()'
-spec update(rangereg_op(), riak_dt:actor(), rangereg()) ->
                    {ok, rangereg()} | nonintegral_error().
update({assign, Value, Ts}, _Actor, OldVal) when is_integer(Value) ->
    {ok, merge(new_range_from_assign(Value, Ts), OldVal)};
update({assign, Value, _Ts}, _Actor, _OldVal) ->
    {error, {type, {nonintegral, Value}}}.

-spec update(rangereg_op(), riak_dt:actor(), rangereg(), riak_dt:context()) ->
                    {ok, rangereg()} | nonintegral_error().
update(Op, Actor, OldVal, _Ctx) ->
    update(Op, Actor, OldVal).

new_range_from_assign(Value, Ts) ->
  #rangereg{max=Value, min=Value, first={Value,Ts}, last={Value,Ts}}.


-spec parent_clock(riak_dt_vclock:vclock(), rangereg()) ->
                          rangereg().
parent_clock(_Clock, RangeReg) ->
    RangeReg.

%% @doc Merge two `rangereg()'s to a single `rangereg()'. This is the Least Upper Bound
%% function described in the literature.
%% We max the maximum, min the minimum, LWW the last, and first-write-wins the first.
%% For LWW, if the timestamps are equal, we take the higher integer. For FWW, we take
%% the lower integer.
-spec merge(rangereg(), rangereg()) -> rangereg().
merge(#rangereg{max=MaxA, min=MinA, first=FirstA, last=LastA},
      #rangereg{max=MaxB, min=MinB, first=FirstB, last=LastB}) ->
    #rangereg{max=max_with_small_undefined(MaxA,MaxB),
               min=min_with_large_undefined(MinA,MinB),
               first=fww(FirstA,FirstB),
               last =lww(LastA,LastB)}.

%% @private
max_with_small_undefined(undefined, X) ->
    X;
max_with_small_undefined(X, undefined) ->
    X;
max_with_small_undefined(X, Y) ->
    max(X, Y).

min_with_large_undefined(undefined, X) ->
    X;
min_with_large_undefined(X, undefined) ->
    X;
min_with_large_undefined(X, Y) ->
    min(X, Y).

fww(undefined, Other) ->
  Other;
fww(Other, undefined) ->
  Other;
fww({VA,TsA}, {_VB,TsB}) when TsA < TsB ->
  {VA,TsA};
fww({_VA,TsA}, {VB,TsB}) when TsA > TsB ->
  {VB,TsB};
fww(A, B) ->
  min(A,B). %% some kind of tiebreaker.

lww(undefined, Other) ->
  Other;
lww(Other, undefined) ->
  Other;
lww({VA,TsA}, {_VB,TsB}) when TsA > TsB ->
  {VA,TsA};
lww({_VA,TsA}, {VB,TsB}) when TsA < TsB ->
  {VB,TsB};
lww(A,B) ->
  max(A,B). %% some kind of tiebreaker.

%% @doc Are two `rangereg()'s structurally equal? Equality here is
%% that both registers contain the same value.
-spec equal(rangereg(), rangereg()) -> boolean().
equal(Val1, Val2) ->
    Val1 =:= Val2.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_RANGEREG_TAG).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of an `rangereg()'
-spec to_binary(rangereg()) -> binary().
to_binary(RR) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(RR))/binary>>.

%% @doc Decode binary `rangereg()'
-spec from_binary(binary()) -> rangereg().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    binary_to_term(Bin).


%% @doc No Stats as everything in the datatype is public
-spec stats(rangereg()) -> [{atom(), number()}].
stats(_RangeReg) ->
    [].

%% @doc No Stats as everything in the datatype is public
-spec stat(atom(), rangereg()) -> number() | undefined.
stat(_, _) -> undefined.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
generate() ->
    ?LET({Op, Actor}, {gen_op(), char()},
         begin
             {ok, RangeReg} = riak_dt_rangereg:update(Op, Actor, riak_dt_rangereg:new()),
             RangeReg
         end).

init_state() ->
    ordsets:new().

gen_op() ->
    {assign, largeint(), largeint()}.

update_expected(_ID, {assign, Val, Ts}, OldVal) ->
    ordsets:add_element({Val,Ts}, OldVal);
update_expected(_ID, _Op, Prev) ->
    Prev.

maxVal(Set) ->
  ordsets:fold(fun({Val,_Ts},Max) ->
      max_with_small_undefined(Val,Max)
    end, undefined, Set).

minVal(Set) ->
  ordsets:fold(fun({Val,_Ts},Min) ->
      min_with_large_undefined(Val,Min)
    end, undefined, Set).

firstVal(Set) ->
  ordsets:fold(fun(Pair,First) ->
      fww(Pair,First)
    end, undefined, Set).

lastVal(Set) ->
  ordsets:fold(fun(Pair,Last) ->
      lww(Pair,Last)
    end, undefined, Set).

eqc_state_value(Val) ->
    [
      {max, maxVal(Val)},
      {min, minVal(Val)},
      {first, pair_val(firstVal(Val))},
      {last, pair_val(lastVal(Val))}
    ].
-endif.
-endif.
