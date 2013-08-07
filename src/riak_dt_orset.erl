%% -------------------------------------------------------------------
%%
%% riak_dt_orset: A convergent, replicated, state based observe remove set
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

-module(riak_dt_orset).

-behaviour(riak_dt).

-export_type([orset/0]).
-opaque orset() :: {orddict:orddict(), orddict:orddict()}.

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, update/3, merge/2, equal/2, to_binary/1, from_binary/1, value/2]).

%% EQC API
-ifdef(EQC).
-export([init_state/0, gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

new() ->
    orddict:new().

value(ORDict0) ->
    ORDict1 = orddict:filter(fun(_Elem,{Add,Rem}) ->
            Tokens = ordsets:subtract(Add,Rem),
            ordsets:size(Tokens) /= 0
        end, ORDict0),
    orddict:fetch_keys(ORDict1).

update({add,Elem}, Actor, ORDict) ->
    Token = unique(Actor),
    add_elem(Elem,Token,ORDict);
update({remove,Elem}, _Actor, ORDict) ->
    remove_elem(Elem, ORDict).

merge(ORDictA, ORDictB) ->
    orddict:merge(fun(_Elem,{AddA,RemA},{AddB,RemB}) ->
            Add = ordsets:union(AddA,AddB),
            Rem = ordsets:union(RemA,RemB),
            {Add,Rem}
        end, ORDictA, ORDictB).

equal(ORDictA, ORDictB) ->
    ORDictA == ORDictB. % Everything inside is ordered, so this should work

%% Private
add_elem(Elem,Token,ORDict) ->
    case orddict:find(Elem,ORDict) of
        {ok, {Add,Rem}} -> Add1 = ordsets:add_element(Token,Add),
                           orddict:store(Elem, {Add1,Rem}, ORDict);
        error           -> Add = ordsets:from_list([Token]),
                           orddict:store(Elem, {Add,ordsets:new()}, ORDict)
    end.

remove_elem(Elem, ORDict) ->
    case orddict:find(Elem,ORDict) of
        {ok, {Add,Rem}} -> Rem1 = ordsets:union(Add,Rem),
                           orddict:store(Elem, {Add,Rem1}, ORDict);
        error           -> ORDict
    end.

unique(Actor) ->
    erlang:phash2({Actor, erlang:now()}).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(init_state(), 1000, ?MODULE))]}.

%% EQC generator
gen_op() ->
    ?LET({Add, Remove}, gen_elems(),
         oneof([{add, Add}, {remove, Remove}])).

gen_elems() ->
    ?LET(A, int(), {A, oneof([A, int()])}).

%% Maybe model qc state as op based?
init_state() ->
    {0, dict:new(), []}.

update_expected(ID, {add, Elem}, {Cnt0, Dict, L}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict), [{ID, {add, Elem}}|L]};
update_expected(ID, {remove, Elem}, {Cnt, Dict, L}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict), [{ID, {remove, Elem, ToRem}}|L]};
update_expected(ID, {merge, SourceID}, {Cnt, Dict, L}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict), [{ID,{merge, SourceID}}|L]};
update_expected(ID, create, {Cnt, Dict, L}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict), [{ID, create}|L]}.

eqc_state_value({_Cnt, Dict, _L}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Values = [ Elem || {Elem, _X} <- sets:to_list(Remaining)],
    lists:usort(Values).
-endif.
-endif.
