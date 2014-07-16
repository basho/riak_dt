%% -------------------------------------------------------------------
%%
%% riak_dt_minreg: A Register that stores the smallest integer assigned into it
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
%% A register that stores the smallest integer that is assigned to it
%% It starts as `undefined`, which is a `bottom` value, but will take
%% on any assigned value as long as it is smaller than the value it had
%% before. Only good for storing integers.
%% @end

-module(riak_dt_minreg).
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

-export_type([minreg/0, minreg_op/0]).

-type minreg() :: integer() | undefined.

-type minreg_op() :: {assign, integer()}.

%% @doc Create a new, empty `minreg()'
-spec new() -> minreg().
new() ->
    undefined.

%% @doc The single value of a `minreg()'.
-spec value(minreg()) -> term().
value(Value) ->
    Value.

%% @doc query this `minreg()'.
-spec value(term(), minreg()) -> term().
value(_, V) ->
    value(V).

%% @doc Assign a `Value' to the `minreg()'
-spec update(minreg_op(), riak_dt:actor(), minreg()) ->
                    {ok, minreg()}.
update({assign, Value}, _Actor, OldVal) when is_integer(Value) ->
    {ok, merge(OldVal, Value)};
update({assign, _Value}, _Actor, _OldVal) ->
    error(badarg).

-spec update(minreg_op(), riak_dt:actor(), minreg(), riak_dt:context()) ->
                    {ok, minreg()}.
update(Op, Actor, OldVal, _Ctx) ->
    update(Op, Actor, OldVal).

-spec parent_clock(riak_dt_vclock:vclock(), minreg()) -> minreg().
parent_clock(_Clock, MinReg) ->
    MinReg.

%% @doc Merge two `minreg()'s to a single `minreg()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(minreg(), minreg()) -> minreg().
merge(Val1, Val2) ->
    min_with_large_undefined(Val1, Val2).

%% @private
min_with_large_undefined(undefined, X) ->
    X;
min_with_large_undefined(X, undefined) ->
    X;
min_with_large_undefined(X, Y) ->
    min(X, Y).

%% @doc Are two `minreg()'s structurally equal? Equality here is
%% that both registers contain the same value.
-spec equal(minreg(), minreg()) -> boolean().
equal(Val1, Val2) ->
    Val1 =:= Val2.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_MINREG_TAG).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of an `minreg()'
-spec to_binary(minreg()) -> binary().
to_binary(MinReg) ->
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(MinReg))/binary>>.

%% @doc Decode binary `minreg()'
-spec from_binary(binary()) -> minreg().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    binary_to_term(Bin).

%% @doc No Stats as it's just an integer
-spec stats(minreg()) -> [{atom(), number()}].
stats(_MinReg) -> [].

%% @doc No Stats as it's just an integer
-spec stat(atom(), minreg()) -> number() | undefined.
stat(_, _) -> undefined.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
bin_roundtrip_test_() ->
    crdt_statem_eqc:run_binary_rt(?MODULE, 1000).

eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
generate() ->
    ?LET({Op, Actor}, {gen_op(), char()},
         begin
             {ok, MinReg} = riak_dt_minreg:update(Op, Actor, riak_dt_minreg:new()),
             MinReg
         end).

init_state() ->
    undefined.

gen_op() ->
    {assign, ?LET(X,largeint(),-X)}.

update_expected(_ID, {assign, Val}, OldVal) ->
    min_with_large_undefined(Val, OldVal);
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(Val) ->
    Val.
-endif.
-endif.
