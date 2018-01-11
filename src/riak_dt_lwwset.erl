%% -------------------------------------------------------------------
%%
%% riak_dt_lwwset: LWW-Element-Set
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

%% @doc Erlang DT implemntation of Roshi's LWW-Element-Set
%%
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek
%% Zawirski (2011) A comprehensive study of Convergent and Commutative
%% Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @reference Roshi, https://github.com/soundcloud/roshi
%%
%% @end
-module(riak_dt_lwwset).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(NUMTESTS, 1000).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2]).
-export([update/3, update/4, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).
-export([to_binary/2]).
-export([stats/1, stat/2]).
-export([parent_clock/2]).
-export([to_version/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, gen_op/1, update_expected/3, eqc_state_value/1]).
-export([init_state/0, generate/0, size/1]).

-endif.

-define(ADD, 1).
-define(REM, 0).

-export_type([lwwset/0, lwwset_op/0, binary_lwwset/0]).

-type lwwset() :: [entry()].

-type binary_lwwset() :: binary(). %% A binary that from_binary/1 will operate on.

-type lwwset_op() ::  {add, member(), ts()} | {remove, member(), ts()}.

-type entry() :: {member(), {ts(), status()}}.

-type member() :: term().
-type ts() :: pos_integer().
-type status() :: add() | remove().
-type add() :: ?ADD.
-type remove() :: ?REM.

-ifdef(EQC).
-define(DICT, orddict).
-else.
-define(DICT, dict).
-endif.

-spec new() -> lwwset().
new() ->
    ?DICT:new().

-spec parent_clock(riak_dt_vclock:vclock(), lwwset()) -> lwwset().
parent_clock(_Clock, LWWSet) ->
    LWWSet.

-spec value(lwwset()) -> [member()].
value(LWWSet) ->
    [K || {K, {_TS, Status}} <- ?DICT:to_list(LWWSet), Status == 1].

value(size, LWWSet) ->
    length(value(LWWSet));
value({contains, Elem}, LWWSet) ->
    lists:member(Elem, value(LWWSet)).

-spec update(lwwset_op(), riak_dt:actor() | riak_dt:dot(), lwwset()) -> {ok, lwwset()}.
update({add, Elem, TS}, _Actor, LWWSet) ->
    {ok, add_elem(Elem, TS, LWWSet)};
update({remove, Elem, TS}, _Actor, LWWSet) ->
    {ok, remove_elem(Elem, TS, LWWSet)}.

update(Op, Actor, Set, _Ctx) ->
    update(Op, Actor, Set).

%% Private
-spec add_elem(member(), ts(), lwwset()) -> lwwset().
add_elem(Elem, TS, LWWSet) ->
    case ?DICT:find(Elem, LWWSet) of
        error ->
            ?DICT:store(Elem, {TS, ?ADD}, LWWSet);
        {ok, {TS, ?REM}} ->
            ?DICT:store(Elem, {TS, ?ADD}, LWWSet);
        {ok, {TS0, _}} when TS0 < TS ->
            ?DICT:store(Elem, {TS, ?ADD}, LWWSet);
        _ ->
            LWWSet
    end.

%% @doc warning, allows doomstoning.
-spec remove_elem(member(), ts(), lwwset()) -> lwwset().
remove_elem(Elem, TS, LWWSet) ->
    case ?DICT:find(Elem, LWWSet) of
        error ->
            ?DICT:store(Elem, {TS, ?REM}, LWWSet);
        {ok, {TS, ?ADD}} ->
            LWWSet;
        {ok, {TS0, _}} when TS0 < TS ->
            ?DICT:store(Elem, {TS, ?REM}, LWWSet);
        _ ->
            LWWSet
    end.

-spec merge(lwwset(), lwwset()) -> lwwset().
merge(LWWSet, LWWSet) ->
    LWWSet;
merge(LWWSet1, LWWSet2) ->
    ?DICT:merge(fun lww/3, LWWSet1, LWWSet2).

lww(_Key, {TS, ?ADD}, {TS, ?REM}) ->
    {TS, ?ADD};
lww(_Key, {TS, ?REM}, {TS, ?ADD}) ->
    {TS, ?ADD};
lww(_Key, {TS, Op}, {TS, Op}) ->
    {TS, Op};
lww(_Key, {TS1, _}=V1, {TS2, _}) when TS1 > TS2  ->
    V1;
lww(_Key, {TS1, _}, {TS2, _}=V2) when TS1 < TS2 ->
    V2.

-spec equal(lwwset(), lwwset()) -> boolean().
equal(LWWSet1, LWWSet2) ->
    LWWSet1  == LWWSet2.

-spec stats(lwwset()) -> [{atom(), number()}].
stats(LWWSet) ->
    [{S, stat(S, LWWSet)} || S <- [element_count]].

-spec stat(atom(), lwwset()) -> number() | undefined.
stat(element_count, LWWSet) ->
    ?DICT:size(LWWSet);
stat(_,_) -> undefined.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_LWWSET_TAG).
-define(V1_VERS, 1).

%% @doc returns a binary representation of the provided
%% `orswot()'. The resulting binary is tagged and versioned for ease
%% of future upgrade. Calling `from_binary/1' with the result of this
%% function will return the original set. Use the application env var
%% `binary_compression' to turn t2b compression on (`true') and off
%% (`false')
%%
%% @see `from_binary/1'
-spec to_binary(lwwset()) -> binary_lwwset().
to_binary(S) ->
    {ok, B} = to_binary(?V1_VERS, S),
    B.

%% @doc encode set to target version. The first argument is the target
%% binary type.
-spec to_binary(Vers :: pos_integer(), lwwset()) -> {ok, binary_lwwset()} | ?UNSUPPORTED_VERSION.
to_binary(?V1_VERS, S) ->
    {ok, <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(S))/binary>>};
to_binary(Vers, _S) ->
    ?UNSUPPORTED_VERSION(Vers).

%% @doc When the argument is a `binary_lwwset()' produced by
%% `to_binary/1' will return the original `lwwset()'.
%%
%% @see `to_binary/1'
-spec from_binary(binary_lwwset()) -> {ok, lwwset()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    {ok, riak_dt:from_binary(B)};
from_binary(<<?TAG:8/integer, Vers:8/integer, _B/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec to_version(pos_integer(), lwwset()) -> lwwset().
to_version(_Version, Set) ->
    Set.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).

bin_roundtrip_test_() ->
    crdt_statem_eqc:run_binary_rt(?MODULE, ?NUMTESTS).

eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, ?NUMTESTS).

generate() ->
    new().

size(Set) ->
    [{element_count, Cnt}] = stats(Set),
    Cnt.

%% EQC generator
gen_op() ->
    ?SIZED(Size, gen_op(Size)).

gen_op(_Size) ->
    oneof([{add, int(), nat()}, {remove, int(), nat()}]).

init_state() ->
    {orddict:new(), orddict:new()}.

update_expected(_ID, {add, Elem, TS}, {A0, R}) ->
    A = update_element(A0, Elem, TS),
    {A, R};
update_expected(_ID, {remove, Elem, TS}, {A, R0}) ->
    R = update_element(R0, Elem, TS),
    {A, R};
update_expected(_, _, S) ->
    S.

update_element(Dict, Elem, TS) ->
    orddict:update(Elem, fun(T) when T >= TS-> T;
                            (_T) -> TS end,
                   TS,
                   Dict).

eqc_state_value({A, R}) ->
    orddict:fold(fun(Elem, TS, Acc) ->
                         case orddict:find(Elem, R) of
                             error ->
                                 [Elem | Acc];
                             {ok, T} when T > TS ->
                                 Acc;
                             _ ->
                                 [Elem | Acc]
                         end
                 end,
                 [],
                 A).
-endif.

stat_test() ->
    Set = new(),
    {ok, Set1} = update({add, <<"foo">>, 1}, 1, Set),
    {ok, Set2} = update({add, <<"foo">>, 2}, 2, Set1),
    {ok, Set3} = update({add, <<"bar">>, 3}, 3, Set2),
    {ok, Set4} = update({remove, <<"foo">>, 4}, 1, Set3),
    ?assertEqual([{element_count, 2}], stats(Set4)).

-endif.
