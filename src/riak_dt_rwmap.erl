%% -------------------------------------------------------------------
%%
%% riak_dt_vvorset: Another convergent, replicated, state based observe remove set
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

-module(riak_dt_rwmap).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2]).
-export([update/3, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-record(field, {active=true,
                clock,
                value,
                tombstone=riak_dt_vclock:fresh()}).

new() ->
    orddict:new().

value(Map) ->
    [{K, Type:value(Field#field.value)} || {{_Name, Type}=K, Field} <- orddict:to_list(Map),
                                           Field#field.active].

value(_, Map) ->
    value(Map).

update({update, Ops}, Actor, Map) ->
    apply_ops(Ops, Actor, Map).

apply_ops([], _Actor, Map) ->
    {ok, Map};
apply_ops([{update, {_Name, Mod}=Key, Op} | Rest], Actor, Map0) ->
    #field{value=Value, clock=Clock}=Field = get_field(orddict:find(Key, Map0), Mod),
    case Mod:update(Op, Actor, Value) of
        {ok, NewValue} ->
            Clock2 = riak_dt_vclock:increment(Actor, Clock),
            Map = orddict:store(Key, Field#field{value=NewValue,
                                                 clock=Clock2}),
            apply_ops(Rest, Actor, Map);
        Error ->
            Error
    end;
apply_ops([{remove, Key} | Rest], Actor, Map) ->
    case tombstone_field(orddict:find(Key, Map), Actor, Key, Map) of
        precondition_failed ->
            {error, {precondition, {not_present, Key}}};
        Map2 ->
            apply_ops(Rest, Actor, Map2)
    end;
apply_ops([{add, Key} | Rest], Actor, Map0) ->
    #field{clock=Clock}=Field = get_field(Key, Map0),
    Field2 = Field#field{clock=riak_dt_vclock:increment(Actor, Clock)},
    Map = orddict:store(Key, Field2, Map0),
    apply_ops(Rest, Actor, Map).


get_field({ok, Field}, _Mod) ->
    Field;
get_field(error, Mod) ->
    #field{value=Mod:new(), clock=riak_dt_vclock:fresh()}.

tombstone_field({ok, Field=#field{active=true}}, Actor, {_Name, Type}=Key, Map) ->
    #field{clock=Clock} = Field,
    F2 = Field#field{active=false,
                     value=Type:new(),
                     tombstone=riak_dt_vclock:increment(Actor, Clock)}, %% Removes win
    orddict:store(Key, F2, Map);
tombstone_field({ok, #field{active=false}}, _Actor, _Key, Map) ->
    %% Can't remove a field that's removed, right?
    Map;
tombstone_field(_, _Actor, _Key, _Map) ->
    precondition_failed.

merge(Map1, Map2) ->
    %% for every field:
    %% if both are removed, bueno!
    %% otherwise
    %%    merge clocks and tombstones, active?
    %%    (Active is defined as clock dominating tombstone)
    orddict:merge(fun(_K, F1=#field{active=false}, F2=#field{active=false}) ->
                          #field{clock=VC1, tombstone=TS1} = F1,
                          #field{clock=VC2, tombstone=TS2} = F2,
                          VC3 = riak_dt_vclock:merge(VC1, VC2),
                          TS3 = riak_dt_vclock:merge(TS1, TS2),
                          #field{active=false, clock=VC3, tombstone=TS3};
                     ({_Name, Type}, F1, F2) ->
                          #field{clock=VC1, value=V1, tombstone=TS1} = F1,
                          #field{clock=VC2, value=V2, tombstone=TS2} = F2,
                          VC3 = riak_dt_vclock:merge(VC1, VC2),
                          TS3 = riak_dt_vclock:merge(TS1, TS2),
                          case riak_dt_vclock:descends(VC3, TS3) of
                              true ->
                                  #field{clock=VC3,
                                         value=Type:merge(V1, V2),
                                         tombstone=TS3,
                                         active=true};
                              false ->
                                    #field{clock=VC3,
                                           value=Type:new(),
                                           active=false,
                                           tombstone=TS3}
                          end
                  end,
                  Map1, Map2).

equal(Map1, Map2) ->
    orddict:fetch_keys(Map1) == orddict:fetch_keys(Map2) andalso pairwise_equals(Map1, Map2).

%% Note, only called when we know that 2 schema are equal.
%% Both dicts therefore have the same set of keys.
pairwise_equals(Map1, Map2) ->
    short_cicuit_equals(orddict:to_list(Map1), Map2).

%% @Private
%% Compare each value. Return false as soon as any pair are not equal.
short_cicuit_equals([], _Map2) ->
    true;
short_cicuit_equals([{{_Name, Type}=Key, F1} | Rest], Map2) ->
    F2 = orddict:fetch(Key, Map2),
    case field_equal(Type, F1, F2) of
        true ->
            short_cicuit_equals(Rest, Map2);
        false ->
            false
    end.

field_equal(Type, F1, F2) ->
    #field{clock=VC1, value=V1, active=A1, tombstone=TS1} = F1,
    #field{clock=VC2, value=V2, active=A2, tombstone=TS2} = F2,
    riak_dt_vclock:equal(VC1, VC2) andalso
        riak_dt_vclock:equal(TS1, TS2) andalso
        A1 == A2 andalso
        Type:equal(V1, V2).

-define(TAG, 75).
-define(V1_VERS, 1).


to_binary(Map) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Map))/binary>>.

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

generate() ->
    ?LET(Members, list(int()),
         lists:foldl(fun(M, Set) ->
                            riak_dt_vvorset:update({add, M}, choose(1, 50), Set) end,
                    riak_dt_vvorset:new(),
                    Members)).

%% EQC generator
gen_op() ->
    ?LET({Add, Remove}, gen_elems(),
         oneof([{add, Add}, {remove, Remove}])).

gen_elems() ->
    ?LET(A, int(), {A, oneof([A, int()])}).

init_state() ->
    {0, dict:new()}.

update_expected(ID, {add, Elem}, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)};
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
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Values = [ Elem || {Elem, _X} <- sets:to_list(Remaining)],
    lists:usort(Values).

-endif.

-endif.
