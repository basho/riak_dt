%% -*- coding: utf-8 -*-
%% -------------------------------------------------------------------
%%
%% riak_dt_gset: A convergent, replicated, state based grow only set
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

%% @doc a Grow Only Set. Items may be added, but never removed.
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek
%% Zawirski (2011) A comprehensive study of Convergent and Commutative
%% Replicated Data Types. [http://hal.upmc.fr/inria-00555588/]
%%
%% @end

-module(riak_dt_gset).

-behaviour(riak_dt).

%% API
-export([new/0, value/1, update/3, merge/2, equal/2,
         to_binary/1, from_binary/1, value/2, stats/1, stat/2]).
-export([update/4, parent_clock/2]).
-export([to_binary/2]).
-export([to_version/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% EQC API
-ifdef(EQC).
-export([init_state/0, gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-export_type([gset/0, binary_gset/0, gset_op/0]).
-opaque gset() :: members().

-type binary_gset() :: binary(). %% A binary that from_binary/1 will operate on.

-type gset_op() :: {add, member()} | {add_all, members()}.

-type actor() :: riak_dt:actor().

-type members() :: ordsets:ordset(member()).
-type member() :: term().

-spec new() -> gset().
new() ->
    ordsets:new().

-spec value(gset()) -> [member()].
value(GSet) ->
    ordsets:to_list(GSet).

%% @doc note: not implemented yet, same as `value/1'
-spec value(any(), gset()) -> [member()].
value(_, GSet) ->
    value(GSet).

%%-spec apply_ops([gset_op()], actor() | dot(), orswot()) ->
%%                       {ok, orswot()} | precondition_error().
apply_ops([], _Actor, ORSet) ->
    {ok, ORSet};
apply_ops([Op | Rest], Actor, ORSet) ->
    case update(Op, Actor, ORSet) of
        {ok, ORSet2} ->
            apply_ops(Rest, Actor, ORSet2);
        Error ->
            Error
    end.

-spec update(gset_op(), actor(), gset()) -> {ok, gset()}.
update({add, Elem}, _Actor, GSet) ->
    {ok, ordsets:add_element(Elem, GSet)};

update({update, Ops}, _Actor, GSet) ->
apply_ops(Ops,_Actor,GSet);

update({add_all, Elems}, _Actor, GSet) ->
    {ok, ordsets:union(GSet, ordsets:from_list(Elems))}.

-spec update(gset_op(), actor(), gset(), riak_dt:context()) ->
                    {ok, gset()}.
update(Op, Actor, GSet, _Ctx) ->
    update(Op, Actor, GSet).

-spec parent_clock(riak_dt_vclock:vclock(), gset()) -> gset().
parent_clock(_Clock, GSet) ->
    GSet.

-spec merge(gset(), gset()) -> gset().
merge(GSet1, GSet2) ->
    ordsets:union(GSet1, GSet2).

-spec equal(gset(), gset()) -> boolean().
equal(GSet1, GSet2) ->
    GSet1 == GSet2.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_GSET_TAG).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

-spec to_binary(gset()) -> binary_gset().
to_binary(GSet) ->
    %%<<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(GSet))/binary>>.
    {ok, B} = to_binary(?V2_VERS, GSet),
    B.

-spec to_binary(Vers :: pos_integer(), gset()) -> {ok, binary()} | ?UNSUPPORTED_VERSION.
to_binary(?V1_VERS, S) ->
    {ok, <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(S))/binary>>};
to_binary(?V2_VERS, S) ->
    {ok, <<?TAG:8/integer, ?V2_VERS:8/integer, (riak_dt:to_binary(S))/binary>>};
to_binary(Vers, _S0) ->
    ?UNSUPPORTED_VERSION(Vers).

-spec from_binary(binary()) -> {ok, gset()} | ?UNSUPPORTED_VERSION | ?INVALID_BINARY.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)};
from_binary(<<?TAG:8/integer, ?V2_VERS:8/integer, Bin/binary>>) ->
    {ok, riak_dt:from_binary(Bin)};
from_binary(<<?TAG:8/integer, Vers:8/integer, _Bin/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

-spec stats(gset()) -> [{atom(), number()}].
stats(GSet) ->
    [ {S, stat(S, GSet)} || S <- [element_count, max_element_size]].

-spec stat(atom(), gset()) -> number() | undefined.
stat(element_count, GSet) ->
    length(GSet);
stat(max_element_size, GSet) ->
    ordsets:fold(
      fun(E, S) ->
              max(erlang:external_size(E), S)
      end, 0, GSet);
stat(_, _) -> undefined.

-spec to_version(pos_integer(), gset()) -> gset().
to_version(_Version, Set) ->
    Set.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

stat_test() ->
    S0 = new(),
    {ok, S1} = update({add_all, [<<"a">>, <<"b1">>, <<"c23">>, <<"d234">>]}, 1, S0),
    ?assertEqual([{element_count, 0}, {max_element_size, 0}], stats(S0)),
    ?assertEqual([{element_count, 4}, {max_element_size, 15}], stats(S1)),
    ?assertEqual(4, stat(element_count, S1)),
    ?assertEqual(15, stat(max_element_size, S1)),
    ?assertEqual(undefined, stat(actor_count, S1)).

to_binary_test() ->
  GSet = update({add, <<"foo">>}, undefined_actor, riak_dt_gset:new()),
  Bin = riak_dt_gset:to_binary(GSet),
  ?assertMatch( <<82:8/integer, ?V2_VERS:8/integer, _/binary>> , Bin).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
gen_op() ->
    oneof([{add, int()},
           {add_all, non_empty(list(int()))}]).

init_state() ->
    dict:new().

update_expected(ID, {add, Elem}, Dict) ->
    dict:update(ID, fun(Set) -> sets:add_element(Elem, Set) end,
                sets:add_element(Elem, sets:new()),
                Dict);
update_expected(ID, {add_all, Elems}, Dict) ->
    dict:update(ID, fun(Set) -> sets:union(sets:from_list(Elems), Set) end,
                sets:from_list(Elems),
                Dict);
update_expected(ID, {merge, SourceID}, Dict) ->
    Replica1 = dict:fetch(SourceID, Dict),
    Replica2 = dict:fetch(ID, Dict),
    NewValue = sets:union(Replica1, Replica2),
    dict:store(ID, NewValue, Dict);
update_expected(ID, create, Dict) ->
    dict:store(ID, sets:new(), Dict).

eqc_state_value(Dict) ->
    S = dict:fold(fun(_K, V, Acc) ->
                          sets:union(V, Acc) end,
                  sets:new(),
                  Dict),
    sets:to_list(S).

-endif.

-endif.
