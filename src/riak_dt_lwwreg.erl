%% -------------------------------------------------------------------
%%
%% riak_dt_lwwreg: A DVVSet based last-write-wins register
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
%% An LWW Register CRDT.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek Zawirski (2011) A comprehensive study of
%% Convergent and Commutative Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(riak_dt_lwwreg).

-export([new/0, value/1, value/2, update/3, merge/2,
         equal/2, to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([lwwreg/0, lwwreg_op/0]).

-opaque lwwreg() :: {term(), non_neg_integer()}.

-type lwwreg_op() :: {assign, term(), non_neg_integer()}.

-type lww_q() :: timestamp.

%% @doc Create a new, empty `lwwreg()'
-spec new() -> lwwreg().
new() ->
    {<<>>, 0}.

%% @doc The single total value of a `gcounter()'.
-spec value(lwwreg()) -> term().
value({Value, _TS}) ->
    Value.

%% @doc query for this `lwwreg()'.
%% `timestamp' is the only query option.
-spec value(lww_q(), lwwreg()) -> non_neg_integer().
value(timestamp, {_V, TS}) ->
    TS.

%% @doc Assign a `Value' to the `lwwreg()'
%% associating the update with time `TS'
-spec update(lwwreg_op(), term(), lwwreg()) ->
                    {ok, lwwreg()}.
update({assign, Value, TS}, _Actor, {_OldVal, OldTS}) when is_integer(TS), TS > 0, TS >= OldTS ->
    {ok, {Value, TS}};
update({assign, _Value, _TS}, _Actor, OldLWWReg) ->
    {ok, OldLWWReg};
%% For when users don't provide timestamps
%% don't think it is a good idea to mix server and client timestamps
update({assign, Value}, _Actor, {OldVal, OldTS}) ->
    MicroEpoch = make_micro_epoch(),
    LWW = case MicroEpoch > OldTS of
              true ->
                  {Value, MicroEpoch};
              false ->
                  {OldVal, OldTS}
          end,
    {ok, LWW}.

make_micro_epoch() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

%% @doc Merge two `lwwreg()'s to a single `lwwreg()'. This is the Least Upper Bound
%% function described in the literature.
-spec merge(lwwreg(), lwwreg()) -> lwwreg().
merge({Val1, TS1}, {_Val2, TS2}) when TS1 > TS2 ->
    {Val1, TS1};
merge({_Val1, TS1}, {Val2, TS2}) when TS2 > TS1 ->
    {Val2, TS2};
merge(LWWReg1, LWWReg2) when LWWReg1 >= LWWReg2 ->
    LWWReg1;
merge(_LWWReg1, LWWReg2) ->
    LWWReg2.

%% @doc Are two `lwwreg()'s structurally equal? This is not `value/1' equality.
%% Two registers might represent the value `armchair', and not be `equal/2'. Equality here is
%% that both registers contain the same value and timestamp.
-spec equal(lwwreg(), lwwreg()) -> boolean().
equal({Val, TS}, {Val, TS}) ->
    true;
equal(_, _) ->
    false.

-define(TAG, 72).
-define(V1_VERS, 1).

%% @doc Encode an effecient binary representation of an `lwwreg()'
-spec to_binary(lwwreg()) -> binary().
to_binary({Val, TS}) ->
    {ValueLen, ValueBin} = val_to_binary(Val),
    {TSLen, TSBin} = ts_to_binary(TS),
    <<?TAG:8/integer, ?V1_VERS:8/integer, ValueLen:8/integer, ValueBin/binary, TSLen:8/integer, TSBin/binary>>.

%% @doc Decode binary `lwwreg()'
-spec from_binary(binary()) -> lwwreg().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, ValueLen:8/integer, ValueBin:ValueLen/binary,
              TSLen:8/integer, TSBin:TSLen/binary>>) ->
    {binary_to_val(ValueBin), binary_to_ts(TSBin)}.

val_to_binary(Val) when is_binary(Val) ->
    Bin = <<1, Val/binary>>,
    {byte_size(Bin), Bin};
val_to_binary(Term) ->
    Bin = <<0, (term_to_binary(Term))/binary>>,
    {byte_size(Bin), Bin}.

binary_to_val(<<1, Val/binary>>) ->
    Val;
binary_to_val(<<0, Val/binary>>) ->
    binary_to_term(Val).

binary_to_ts(<<1, TS/binary>>) ->
    binary:decode_unsigned(TS);
binary_to_ts(TS) ->
    binary_to_val(TS).

ts_to_binary(TS) when is_integer(TS) ->
    Bin = <<1, (binary:encode_unsigned(TS))/binary>>,
    {byte_size(Bin), Bin};
ts_to_binary(TS) ->
    val_to_binary(TS).

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
             {ok, Lww} = riak_dt_lwwreg:update(Op, Actor, riak_dt_lwwreg:new()),
             Lww
         end).

init_state() ->
    {<<>>, 0}.

gen_op() ->
    ?LET(TS, largeint(), {assign, binary(), abs(TS)}).

update_expected(_ID, {assign, Val, TS}, {OldVal, OldTS}) ->
    case TS >= OldTS of
        true ->
            {Val, TS};
        false ->
            {OldVal, OldTS}
    end;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value({Val, _TS}) ->
    Val.
-endif.

new_test() ->
    ?assertEqual({<<>>, 0}, new()).

value_test() ->
    Val1 = "the rain in spain falls mainly on the plane",
    LWWREG1 = {Val1, 19090},
    LWWREG2 = new(),
    ?assertEqual(Val1, value(LWWREG1)),
    ?assertEqual(<<>>, value(LWWREG2)).

update_assign_test() ->
    LWW0 = new(),
    {ok, LWW1} = update({assign, value1, 2}, actor1, LWW0),
    {ok, LWW2} = update({assign, value0, 1}, actor1, LWW1),
    ?assertEqual({value1, 2}, LWW2),
    {ok, LWW3} = update({assign, value2, 3}, actor1, LWW2),
    ?assertEqual({value2, 3}, LWW3).

update_assign_ts_test() ->
    LWW0 = new(),
    {ok, LWW1} = update({assign, value0}, actr, LWW0),
    {ok, LWW2} = update({assign, value1}, actr, LWW1),
    ?assertMatch({value1, _}, LWW2).

merge_test() ->
    LWW1 = {old_value, 3},
    LWW2 = {new_value, 4},
    ?assertEqual({<<>>, 0}, merge(new(), new())),
    ?assertEqual({new_value, 4}, merge(LWW1, LWW2)),
    ?assertEqual({new_value, 4}, merge(LWW2, LWW1)).

equal_test() ->
    LWW1 = {value1, 1000},
    LWW2 = {value1, 1000},
    LWW3 = {value1, 1001},
    LWW4 = {value2, 1000},
    ?assertNot(equal(LWW1, LWW3)),
    ?assert(equal(LWW1, LWW2)),
    ?assertNot(equal(LWW4, LWW1)).

roundtrip_bin_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, 2}, a1, LWW),
    {ok, LWW2} = update({assign, 4}, a2, LWW1),
    {ok, LWW3} = update({assign, 89}, a3, LWW2),
    {ok, LWW4} = update({assign, <<"this is a binary">>}, a4, LWW3),
    Bin = to_binary(LWW4),
    Decoded = from_binary(Bin),
    ?assert(equal(LWW4, Decoded)).

query_test() ->
    LWW = new(),
    {ok, LWW1} = update({assign, value, 100}, a1, LWW),
    ?assertEqual(100, value(timestamp, LWW1)).

-endif.
