%% -------------------------------------------------------------------
%%
%% riak_dt_map: OR-Set schema based multi CRDT container
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

%% @doc a multi CRDT holder. A Document-ish thing. Consists of two
%% elements, a Schema and a value list. The schema is an OR-Set of
%% {name, type} tuples that identify a field and it's type (type must
%% be a CRDT module, yes, even this one.)  The value list is a dict of
%% {name, type} -> CRDT value mappings. It uses a tombstoneless OR-Set
%% for keys, so we drop deleted values at once.
%%
%%
%% @end

-module(riak_dt_map).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, update/3, merge/2,
         equal/2, to_binary/1, from_binary/1, precondition_context/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1,
         init_state/0, gen_field/0]).
-endif.

-export_type([map/0, binary_map/0, map_op/0]).

-type binary_map() :: binary(). %% A binary that from_binary/1 will accept
-type map() :: {schema(), valuelist()}.
-type schema() :: riak_dt_orswot:orswot().
-type field() :: {Name::term(), Type::crdt_mod()}.
-type crdt_mod() :: riak_dt_pncounter | riak_dt_lwwreg |
                    riak_dt_od_flag |
                    riak_dt_map | riak_dt_orswot.
-type valuelist() :: [{field(), crdt()}].

-type crdt()  ::  riak_dt_pncounter:pncounter() | riak_dt_od_flag:od_flag() |
                  riak_dt_lwwreg:lwwreg() |
                  riak_dt_orswot:orswot() |
                  riak_dt_map:map().

-type map_op() :: {update, [map_field_update() | map_field_op()]}.

-type map_field_op() :: {add, field()} | {remove, field()}.
-type map_field_update() :: {update, field(), crdt_op()}.

-type crdt_op() :: riak_dt_pncounter:pncounter_op() |
                   riak_dt_lwwreg:lwwreg_op() |
                   riak_dt_orswot:orswot_op() | riak_dt_od_flag:od_flag_op() |
                   riak_dt_map:map_op().

-type map_q() :: size | {get, field()} | {get_crdt, field()} |
                   keyset | {contains, field()}.

-type values() :: [value()].
-type value() :: {field(), riak_dt_map:values() | integer() | [term()] | boolean() | term()}.
-type precondition_error() :: {error, {precondition, {not_present, field()}}}.

%% @doc Create a new, empty Map.
-spec new() -> map().
new() ->
    {riak_dt_orswot:new(), orddict:new()}.

%% @doc get the current set of values for this Map
-spec value(map()) -> values().
value({Schema, Values}) ->
    PresentFields = riak_dt_orswot:value(Schema),
    values(PresentFields, Values).

%% @private
values(PresentFields, Values) ->
    values(PresentFields, Values, []).

%% @private
values([], _Values, Acc) ->
    Acc;
values([{_Name, Mod}=Key | Rest], Values, Acc) ->
    CRDT = orddict:fetch(Key, Values),
    Val = Mod:value(CRDT),
    values(Rest, Values, [{Key, Val} | Acc]).

%% @doc execute the given `map_q()' against the given
%% `map()'.
%% @TODO add a query for getting a subset of fields
%% including submap fields (Maybe kvc like?)
-spec value(map_q(), map()) -> term().
value(size, {Schema, _Values}) ->
    riak_dt_orswot:value(size, Schema);
value({get, {_Name, Mod}=Field}, Map) ->
    case value({get_crdt, Field}, Map) of
        error -> error;
        CRDT -> Mod:value(CRDT)
    end;
value({get_crdt, Field}, {Schema, Values}) ->
    case riak_dt_orswot:value({contains, Field}, Schema) of
        true ->
            orddict:fetch(Field, Values);
        false ->
            error
    end;
value(keyset, {Schema, _Values}) ->
    riak_dt_orswot:value(Schema);
value({contains, Field}, {Schema, _Values}) ->
    riak_dt_orswot:value({contains, Field}, Schema).

%% @Doc update the `map()' or a field in the `map()' by executing
%% the `map_op()'. `Ops' is a list of one or more of the following
%% ops:
%%
%% `{update, field(), Op} where `Op' is a valid update operation for a
%% CRDT of type `Mod' from the `Key' pair `{Name, Mod}' If there is no
%% local value for `Key' a new CRDT is created, the operation applied
%% and the result inserted otherwise, the operation is applied to the
%% local value.
%%
%% {add, `field()'}' where field is `{name, type}' results in `field'
%% being added to the Map, and a new crdt of `type' being its value.
%% `{remove, `field()'}' where field is `{name, type}', results in the
%% crdt at `field' and the key and value being removed. A concurrent
%% `update' | `add' will win over a remove.
%%
%% Either all of `Ops' are performed successfully, or none are.
%%
%% @see riak_dt_orswot for more details.

-spec update(map_op(), riak_dt:actor(), map()) -> {ok, map()} | precondition_error().
update({update, Ops}, Actor, {Schema, Values}) ->
    apply_ops(Ops, Actor, Schema, Values).

%% @Private
apply_ops([], _Actor, Schema, Values) ->
    {ok, {Schema, Values}};
apply_ops([{update, {_Name, Mod}=Key, Op} | Rest], Actor, Schema, Values) ->
    {ok, NewSchema} = riak_dt_orswot:update({add, Key}, Actor, Schema),
    InitialValue = fetch_with_default(Key, Values, Mod:new()),
    case Mod:update(Op, Actor, InitialValue) of
        {ok, NewValue} ->
            NewValues = orddict:store(Key, NewValue, Values),
            apply_ops(Rest, Actor, NewSchema, NewValues);
        Error ->
            Error
    end;
apply_ops([{remove, Key} | Rest], Actor, Schema, Values) ->
    case riak_dt_orswot:update({remove, Key}, Actor, Schema) of
        {ok, NewSchema} ->
            apply_ops(Rest, Actor, NewSchema, orddict:erase(Key, Values));
        _Error -> {error, {precondition, {not_present, Key}}}
    end;
apply_ops([{add, {_Name, Mod}=Key} | Rest], Actor, Schema, Values) ->
    {ok, NewSchema} = riak_dt_orswot:update({add, Key}, Actor, Schema),
    NewValues = orddict:update(Key, fun(V) -> V end, Mod:new(), Values),
    apply_ops(Rest, Actor, NewSchema, NewValues).

fetch_with_default(Key, Values, Default) ->
    fetch_with_default(orddict:find(Key, Values), Default).

fetch_with_default({ok, Value}, _Default) ->
    Value;
fetch_with_default(error, Default) ->
    Default.

%% @Doc merge two `map()'s.  Performs a merge on the key set (schema)
%% and then a pairwise merge on all values in the value list.  This is
%% the LUB function.
-spec merge(map(), map()) -> map().
merge({Schema1, Values1}, {Schema2, Values2}) ->
    NewSchema = riak_dt_orswot:merge(Schema1, Schema2),
    %% Only merge values present in new schema
    %% @TODO IN FACT only merge things where the dots are different in the OR-Set field
    PresentFields = riak_dt_orswot:value(NewSchema),
    NewValues = merge_values(PresentFields, Values1, Values2, []),
    {NewSchema, NewValues}.

%% @Private
merge_values([], _Values1, _Values2, Acc) ->
    orddict:from_list(Acc);
merge_values([{_Name, Mod}=Key | Rest], Values1, Values2, Acc) ->
    V1 = type_safe_fetch(Mod, orddict:find(Key, Values1)),
    V2 = type_safe_fetch(Mod, orddict:find(Key, Values2)),
    V = Mod:merge(V1, V2),
    merge_values(Rest, Values1, Values2, [{Key, V} | Acc]).

%% @Private
type_safe_fetch(_Mod, {ok, Value}) ->
    Value;
type_safe_fetch(Mod, error) ->
    Mod:new().

%% @Doc compare two `map()'s for equality of structure Both schemas
%% and value list must be equal. Performs a pariwise equals for all
%% values in the value lists
-spec equal(map(), map()) -> boolean().
equal({Schema1, Values1}, {Schema2, Values2}) ->
    riak_dt_orswot:equal(Schema1, Schema2)  andalso pairwise_equals(Values1, Values2).

%% @Private Note, only called when we know that 2 schema are equal.
%% Both dicts therefore have the same set of keys.
pairwise_equals(Values1, Values2) ->
    short_cicuit_equals(orddict:to_list(Values1), Values2).

%% @Private Compare each value. Return false as soon as any pair are
%% not equal.
short_cicuit_equals([], _Values2) ->
    true;
short_cicuit_equals([{{_Name, Type}=Key, Val1} | Rest], Values2) ->
    Val2 = orddict:fetch(Key, Values2),
    case Type:equal(Val1, Val2) of
        true ->
            short_cicuit_equals(Rest, Values2);
        false ->
            false
    end.

%% @Doc a "fragment" of the Map that can be used for precondition
%% operations. The schema is just the active Key Set The values are
%% just those values that are present We use either the values
%% precondition_context or the whole CRDT
-spec precondition_context(map()) -> map().
precondition_context({KeySet0, Values0}) ->
    KeySet = riak_dt_orswot:precondition_context(KeySet0),
    Present = riak_dt_orswot:value(KeySet),
    Values = precondition_context(Present, Values0, orddict:new()),
    {KeySet, Values}.

precondition_context([], _, Acc) ->
    Acc;
precondition_context([{_Name, Type}=Field | Rest], Values, Acc) ->
    V = orddict:fetch(Field, Values),
    Ctx = precondition_context(Type, V),
    precondition_context(Rest, Values, orddict:store(Field, Ctx, Acc)).

precondition_context(Type, V) ->
    case lists:member({precondition_context, 1}, Type:module_info(exports)) of
        true ->
            Type:precondition_context(V);
        false ->
            V   %% if there is no smaller precondition context, use the whole value
                %% I suspect a smarter thing would be use nothing, or
                %% the value/1 value But that makes update more
                %% complex (the context would no longer be a CRDT)
    end.

-define(TAG, 77).
-define(V1_VERS, 1).

-spec to_binary(map()) -> binary_map().
to_binary(Map) ->
    %% @TODO something smarter (recurse down valulist calling to_binary?)
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Map))/binary>>.

-spec from_binary(binary_map()) -> map().
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

%% ===================================
%% crdt_statem_eqc callbacks
%% ===================================
gen_op() ->
    ?LET(Ops, non_empty(list(gen_update())), {update, Ops}).

gen_update() ->
    ?LET(Field, gen_field(),
         oneof([{add, Field}, {remove, Field},
                {update, Field, gen_field_op(Field)}])).

gen_field() ->
    {binary(), oneof([riak_dt_pncounter, riak_dt_orswot,
                      riak_dt_lwwreg,
                      riak_dt_map])}.

gen_field_op({_Name, Type}) ->
    Type:gen_op().

init_state() ->
    {0, dict:new()}.

update_expected(ID, {update, Ops}, State) ->
    %% Ops are atomic, all pass or all fail
    %% return original state if any op failed
    update_all(ID, Ops, State);
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
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res)].

%% @private
%% @doc Apply the list of update operations to the model
update_all(ID, Ops, OriginalState) ->
    update_all(ID, Ops, OriginalState, OriginalState).

update_all(_ID, [], _OriginalState, NewState) ->
    NewState;
update_all(ID, [{update, {_Name, Type}=Key, Op} | Rest], OriginalState, {Cnt0, Dict}) ->
    CurrentValue = get_for_key(Key, ID, Dict),
    %% handle precondition errors any precondition error means the
    %% state is not changed at all
    case Type:update(Op, ID, CurrentValue) of
        {ok, Updated} ->
            Cnt = Cnt0+1,
            ToAdd = {Key, Updated, Cnt},
            {A, R} = dict:fetch(ID, Dict),
            update_all(ID, Rest, OriginalState, {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)});
        _Error ->
            OriginalState
    end;
update_all(ID, [{remove, Field} | Rest], OriginalState, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    In = sets:subtract(A, R),
    PresentFields = [E ||  {E, _, _X} <- sets:to_list(In)],
    case lists:member(Field, PresentFields) of
        true ->
            ToRem = [{E, V, X} || {E, V, X} <- sets:to_list(A), E == Field],
            NewState2 = {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)},
            update_all(ID, Rest, OriginalState, NewState2);
        false ->
            OriginalState
    end;
update_all(ID, [{add, {_Name, Type}=Field} | Rest], OriginalState, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Field, Type:new(), Cnt},
    {A, R} = dict:fetch(ID, Dict),
    NewState = {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)},
    update_all(ID, Rest, OriginalState, NewState).


get_for_key({_N, T}=K, ID, Dict) ->
    {A, R} = dict:fetch(ID, Dict),
    Remaining = sets:subtract(A, R),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    proplists:get_value(K, dict:to_list(Res), T:new()).

-endif.

query_test() ->
    Map = new(),
    {ok, Map1} = update({update, [{add, {c, riak_dt_pncounter}},
                                  {add, {s, riak_dt_orswot}},
                                  {add, {m, riak_dt_map}},
                                  {add, {l, riak_dt_lwwreg}},
                                  {add, {l2, riak_dt_lwwreg}}]}, a1, Map),
    ?assertEqual(5, value(size, Map1)),

    {ok, Map2} = update({update, [{remove, {l2, riak_dt_lwwreg}}]}, a2, Map1),
    ?assertEqual(4, value(size, Map2)),

    ?assertEqual([{c, riak_dt_pncounter},
                  {l, riak_dt_lwwreg},
                  {m, riak_dt_map},
                  {s, riak_dt_orswot}], value(keyset, Map2)),

    ?assert(value({contains, {c, riak_dt_pncounter}}, Map2)),
    ?assertNot(value({contains, {l2, riak_dt_lwwreg}}, Map2)),

    {ok, Map3} = update({update, [{update, {c, riak_dt_pncounter}, {increment, 33}},
                            {update, {l, riak_dt_lwwreg}, {assign, lww_val, 77}}]}, a3, Map2),

    ?assertEqual(33, value({get, {c, riak_dt_pncounter}}, Map3)),
    ?assertEqual({lww_val, 77}, value({get_crdt, {l, riak_dt_lwwreg}}, Map3)).

-endif.
