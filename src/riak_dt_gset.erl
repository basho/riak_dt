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
%% Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end

-module(riak_dt_gset).

-behaviour(riak_dt).

%% API
-export([new/0, value/1, update/3, merge/2, equal/2,
         to_binary/1, from_binary/1, value/2]).

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

-type gset_op() :: {add, member()}.

-type actor() :: riak_dt:actor().

-type members() :: orddict:orddict(member()).
-type member() :: term().

-spec new() -> gset().
new() ->
    ordsets:new().

-spec value(gset()) -> [member()].
value(GSet) ->
    ordsets:to_list(GSet).

%% @Doc note: not implemented yet, same as `value/1'
-spec value(any(), gset()) -> [member()].
value(_, GSet) ->
    value(GSet).


-spec update(gset_op(), actor(), gset()) -> {ok, gset()}.
update({add, Elem}, _Actor, GSet) ->
    {ok, ordsets:add_element(Elem, GSet)};
update({add_all, Elems}, _Actor, GSet) ->
    {ok, ordsets:union(GSet, ordsets:from_list(Elems))}.


-spec merge(gset(), gset()) -> gset().
merge(GSet1, GSet2) ->
    ordsets:union(GSet1, GSet2).

-spec equal(gset(), gset()) -> boolean().
equal(GSet1, GSet2) ->
    GSet1 == GSet2.

-define(TAG, 79).
-define(V1_VERS, 1).

-spec to_binary(gset()) -> binary_gset().
to_binary(GSet) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(GSet))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

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
