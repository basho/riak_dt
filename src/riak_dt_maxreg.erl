%% -------------------------------------------------------------------
%%
%% riak_dt_maxreg: A Register that stores the largest integer assigned into it
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
%% A register that stores the largest integer that is assigned to it
%% It starts as `undefined`, which is a `bottom` value, but will take
%% on any assigned value as long as it is larger than the value it had
%% before. Only good for storing integers.
%% @end

-module(riak_dt_maxreg).
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

-export_type([maxreg/0, maxreg_op/0]).

-type maxreg() :: integer() | undefined.

-type maxreg_op() :: {assign, integer()}.

-type nonintegral_error() :: {error, {type, {nonintegral, term()}}}.

%% @doc Create a new, empty `maxreg()'
-spec new() -> maxreg().
new() ->
    undefined.

%% @doc The single value of a `maxreg()'.
-spec value(maxreg()) -> term().
value(Value) ->
    Value.

%% @doc query this `maxreg()'.
-spec value(term(), maxreg()) -> term().
value(_, V) ->
    value(V).

%% @doc Assign a `Value' to the `maxreg()'
-spec update(maxreg_op(), riak_dt:actor(), maxreg()) ->
                    {ok, maxreg()} | nonintegral_error().
update({assign, Value}, _Actor, OldVal) when is_integer(Value) ->
    {ok, merge(OldVal, Value)};
update({assign, Value}, _Actor, _OldVal) ->
    {error, {type, {nonintegral, Value}}}.

-spec update(maxreg_op(), riak_dt:actor(), maxreg(), riak_dt:context()) ->
                    {ok, maxreg()} | nonintegral_error().
update(Op, Actor, MaxReg, _Ctx) ->
    update(Op, Actor, MaxReg).

-spec parent_clock(riak_dt_vclock:vclock(), maxreg()) ->
                          maxreg().
parent_clock(_Clock, MaxReg) ->
    MaxReg.

%% @doc Merge two `maxreg()'s to a single `maxreg()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(maxreg(), maxreg()) -> maxreg().
merge(Val1, Val2) ->
    max_with_small_undefined(Val1, Val2).

%% @private
max_with_small_undefined(undefined, X) ->
    X;
max_with_small_undefined(X, undefined) ->
    X;
max_with_small_undefined(X, Y) ->
    max(X, Y).

%% @doc Are two `maxreg()'s structurally equal? Equality here is
%% that both registers contain the same value.
-spec equal(maxreg(), maxreg()) -> boolean().
equal(Val1, Val2) ->
    Val1 =:= Val2.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_MAXREG_TAG).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of an `maxreg()'
-spec to_binary(maxreg()) -> binary().
to_binary(MaxReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(MaxReg))/binary>>.

%% @doc Decode binary `maxreg()'
-spec from_binary(binary()) -> maxreg().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    binary_to_term(Bin).


%% @doc No Stats because it's just an integer
-spec stats(maxreg()) -> [{atom(), number()}].
stats(_MaxReg) -> [].

%% @doc No Stats because it's just an integer
-spec stat(atom(), maxreg()) -> number() | 'undefined'.
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
             {ok, MaxReg} = riak_dt_maxreg:update(Op, Actor, riak_dt_maxreg:new()),
             MaxReg
         end).

init_state() ->
    undefined.

gen_op() ->
    {assign, largeint()}.

update_expected(_ID, {assign, Val}, OldVal) ->
    max_with_small_undefined(Val, OldVal);
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(Val) ->
    Val.
-endif.
-endif.
