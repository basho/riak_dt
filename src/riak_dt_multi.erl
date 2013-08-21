%% -------------------------------------------------------------------
%%
%% riak_dt_multi: OR-Set schema based multi CRDT container
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

%% @doc a multi CRDT holder.  A Document-ish thing.  Consists of two
%% elements, a Schema and a value list. The schema is an OR-Set of
%% {name, type} tuples that identify a field and it's type (type must
%% be a CRDT module, yes, even this one.)  The value list is a dict of
%% {name, type} -> CRDT value mappings.  The value list may have it's
%% CRDT value removed as soon as the key is removed from the
%% keyset. The Keyset requries consensus for GC.
%%
%% @end

-module(riak_dt_multi).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, update/3, merge/2, reset/2,
         equal/2, to_binary/1, from_binary/1, precondition_context/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1,
         init_state/0, generate/0, gen_field/0]).
-endif.

-export_type([multi/0, binary_multi/0]).

-type binary_multi() :: binary(). %% A binary that from_binary/1 will accept
-type multi() :: {schema(), valuelist()}.
-type schema() :: riak_dt_vvorset:vvorset().
-type field() :: {Name::term(), Type::crdt_mod()}.
-type crdt_mod() :: riak_dt_gcounter | riak_dt_pncounter | riak_dt_lwwreg |
                    riak_dt_orset | riak_dt_vvorset | riak_dt_zorset |
                    riak_dt_multi.
-type valuelist() :: [{field(), crdt()}].

-type crdt()  :: riak_dt_gcounter:gcounter() | riak_dt_pncounter:pncounter() |
                 riak_dt_lwwreg:lwwreg() | riak_dt_orset:orset() |
                 riak_dt_vvorset:vvorset() | riak_dt_zorset:zorset() |
                 riak_dt_multi:multi().

-type multi_op() :: {update, [multi_field_update() | multi_field_op()]}.

-type multi_field_op() :: {add, field()} | {remove, field()}.
-type multi_field_update() :: {update, field(), crdt_op()}.

-type crdt_op() :: riak_dt_gcounter:gcounter_op() | riak_dt_pncounter:pncounter_op() |
                   riak_dt_lwwreg:lwwreg_op() | riak_dt_orset:orset_op() |
                   riak_dt_vvorset:vvorset_op() | riak_dt_zorset:zorset_op() |
                   riak_dt_multi:multi_op().

-type multi_q() :: size | {get, field()} | {get_crdt, field()} |
                   keyset | {contains, field()}.

-spec new() -> multi().
new() ->
    {riak_dt_vvorset:new(), orddict:new()}.

%% @doc get the current set of values for this multi
value({Schema, Values}) ->
    PresentFields = riak_dt_vvorset:value(Schema),
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

%% @doc execute the given `multi_q()' against the given
%% `multi()'.
%% @TODO add a query for getting a subset of fields
%% including submap fields (Maybe kvc like?)
-spec value(multi_q(), multi()) -> term().
value(size, {Schema, _Values}) ->
    riak_dt_vvorset:value(size, Schema);
value({get, {_Name, Mod}=Field}, Map) ->
    case value({get_crdt, Field}, Map) of
        error -> error;
        CRDT -> Mod:value(CRDT)
    end;
value({get_crdt, Field}, {Schema, Values}) ->
    case riak_dt_vvorset:value({contains, Field}, Schema) of
        true ->
            orddict:fetch(Field, Values);
        false ->
            error
    end;
value(keyset, {Schema, _Values}) ->
    riak_dt_vvorset:value(Schema);
value({contains, Field}, {Schema, _Values}) ->
    riak_dt_vvorset:value({contains, Field}, Schema).

%% @Doc update the `multi()' or a field in the `multi()' by executing
%% the `multi_op()'.
%% `Ops' is a list of one or more of the following ops:
%% `{update, field(), Op}' where `Op' is any operation supported by the CRDT mod
%% referenced in the `field()' (`{name, type}'). The `Op' is applied to the value in the value list
%% for the given key.
%% `{insert, field(), `Value'} where Value is a `CRDT' of type `Mod' from the `Key' pair
%% `{Name, Mod}' If there is no local value for `Key' the CRDT is inserted
%% otherwise, it is merged with the local value.
%% `{add, field}' where field is `{name, type}'.
%% `{remove, field}' where field is `{name, type}'
%% These last too are just the OR-Set operations add/remove on the schema.
update({update, Ops}, Actor, {Schema, Values}) ->
    apply_ops(Ops, Actor, Schema, Values).

%% @Private
apply_ops([], _Actor, Schema, Values) ->
    {Schema, Values};
apply_ops([{update, {_Name, Mod}=Key, Op} | Rest], Actor, Schema, Values) ->
    %% Add the key to schema
    NewSchema = riak_dt_vvorset:update({add, Key}, Actor, Schema),
    %% Update the value
    InitialValue0 = Mod:new(),
    InitialValue = Mod:update(Op, Actor, InitialValue0),
    NewValues = orddict:update(Key, update_fun(Mod, Op, Actor), InitialValue, Values),
    apply_ops(Rest, Actor, NewSchema, NewValues);
apply_ops([{remove, {_Name, Type}=Key} | Rest], Actor, Schema, Values) ->
    NewSchema = riak_dt_vvorset:update({remove, Key}, Actor, Schema),
    %% Do a reset remove, we keep the value as a tombstone, but we negate it (as best we can)
    Current = value({get_crdt, Key}, {Schema, Values}),
    NewValues = case Current of
                error -> Values;
                CRDT ->
                    Tombstone = Type:reset(CRDT, Actor),
                    orddict:store(Key, Tombstone, Values)
            end,
    apply_ops(Rest, Actor, NewSchema, NewValues);
apply_ops([{insert, {_Name, Mod}=Key, CRDT} | Rest], Actor, Schema, Values) ->
    %% Add the key to the schema
    %% Update the value, setting value to `CRDT' or Merging
    %% if key is already present
    NewSchema = riak_dt_vvorset:update({add, Key}, Actor, Schema),
    NewValues = orddict:update(Key, fun(V) -> Mod:merge(V, CRDT) end,
                               CRDT, Values),
    apply_ops(Rest, Actor, NewSchema, NewValues).

%% @Private
update_fun(Mod, Op, Actor) ->
    fun(CRDT) ->
            Mod:update(Op, Actor, CRDT)
    end.

%% @Doc merge two `multi()'s.
%% Performs a merge on the key set (schema) and then a pairwise
%% merge on all values in the value list.
%% This is the LUB function.
-spec merge(multi(), multi()) -> multi().
merge({Schema1, Values1}, {Schema2, Values2}) ->
    NewSchema = riak_dt_vvorset:merge(Schema1, Schema2),
    PresentFields = riak_dt_vvorset:value(NewSchema),
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


%% @Doc compare two `multi()'s for equality of  structure
%% Both schemas and value list must be equal.
%% Performs a pariwise equals for all values in the value lists
-spec equal(multi(), multi()) -> boolean().
equal({Schema1, Values1}, {Schema2, Values2}) ->
    riak_dt_vvorset:equal(Schema1, Schema2)  andalso pairwise_equals(Values1, Values2).

%% @Private
%% Note, only called when we know that 2 schema are equal.
%% Both dicts therefore have the same set of keys.
pairwise_equals(Values1, Values2) ->
    short_cicuit_equals(orddict:to_list(Values1), Values2).

%% @Private
%% Compare each value. Return false as soon as any pair are not equal.
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

%% @Doc reset the Map as though it were empty.
%% Essentially remove all present fields.
-spec reset(multi(), term()) -> multi().
reset(Map, Actor) ->
    Fields = value(keyset, Map),
    reset(Fields, Actor, Map).

reset([], _Actor, Map) ->
    Map;
reset([Field | Rest], Actor, Map) ->
    reset(Rest, Actor, update({remove, Field}, Actor, Map)).

%% @Doc a fragment of the Map that can be used for
%% precondition operations.
%% The schema is just the active Key Set
%% The values are just those values that are present
%% We can use either the values precondition_context
%% or the whole CRDT
-spec precondition_context(multi()) -> binary_multi().
precondition_context({KeySet0, Values0}) ->
    KeySet = riak_dt_vvorset:from_binary(riak_dt_vvorset:precondition_context(KeySet0)),
    Present = riak_dt_vvorset:value(KeySet),
    Values = precondition_context(Present, Values0, orddict:new()),
    to_binary({KeySet, Values}).

precondition_context([], _, Acc) ->
    Acc;
precondition_context([{_Name, Type}=Field | Rest], Values, Acc) ->
    V = orddict:fetch(Field, Values),
    Ctx = precondition_context(Type, V),
    precondition_context(Rest, Values, orddict:store(Field, Ctx, Acc)).

precondition_context(Type, V) ->
    case lists:member(precondition_context, Type:module_info(exports)) of
        true ->
            Type:precondition_context(V);
        false ->
            V   %% if there is no smaller precondition context, use the whole value
                %% I suspect a smarter tihng would be use nothing, or
                %% the value/1 value But that makes update more
                %% complex (the context would no longer be a CRDT)
    end.

-define(TAG, 77).
-define(V1_VERS, 1).

to_binary(Map) ->
    %% @TODO something smarter (recurse down valulist calling to_binary?)
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

%% EQC generator
gen_op() ->
    ?LET(Ops, non_empty(list(gen_update())), {update, Ops}).

gen_update() ->
    ?LET(Field, gen_field(),
         oneof([{add, Field}, {remove, Field},
                {update, Field, gen_field_op(Field)}])).

gen_field() ->
    {binary(), oneof([riak_dt_pncounter, riak_dt_vvorset,
                      riak_dt_lwwreg,
                      riak_dt_multi])}.

gen_field_op({_Name, Type}) ->
    Type:gen_op().

generate() ->
    ?LET(Fields, list(gen_field()),
         begin
             Values = [{F, Type:generate()} || {_Name, Type}=F <- Fields, Type /= riak_dt_multi], %% @TODO fix this, deadlocks otherwise
             lists:foldl(fun({K, V}, {FSet, VDict}) ->
                                 Fields2 = riak_dt_vvorset:update({add, K}, choose(1, 50), FSet),
                                 Values2 = orddict:store(K, V, VDict),
                                 {Fields2, Values2} end,
                         riak_dt_multi:new(),
                         Values)
                 end).

init_state() ->
    {0, dict:new()}.

update_expected(ID, {update, Ops}, Values) ->
    lists:foldl(fun(Op, V) ->
                        update_expected(ID, Op, V) end,
                Values,
                Ops);

update_expected(ID, {add, {_Name, Type}=Elem}, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Type:new(), Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, V, X} || {E, V, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)};
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)};
update_expected(ID, {update, {_Name, Type}=Key, Op}, {Cnt0, Dict}) ->
    CurrentValue = get_for_key(Key, ID,  Dict),
    Updated = Type:update(Op, ID, CurrentValue),
    Cnt = Cnt0+1,
    ToAdd = {Key, Updated, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {insert, {_Name, Type}=Key, Value}, {Cnt0, Dict}) ->
    CurrentValue = get_for_key(Key, ID,  Dict),
    Updated = Type:merge(Value, CurrentValue),
    Cnt = Cnt0+1,
    ToAdd = {Key, Updated, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)}.

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
    Map1 = update({update, [{add, {c, riak_dt_pncounter}},
                            {add, {s, riak_dt_vvorset}},
                            {add, {m, riak_dt_multi}},
                            {add, {l, riak_dt_lwwreg}},
                            {add, {l2, riak_dt_lwwreg}}]}, a1, Map),
    ?assertEqual(5, value(size, Map1)),

    Map2 = update({update, [{remove, {l2, riak_dt_lwwreg}}]}, a2, Map1),
    ?assertEqual(4, value(size, Map2)),

    ?assertEqual([{c, riak_dt_pncounter},
                  {l, riak_dt_lwwreg},
                  {m, riak_dt_multi},
                  {s, riak_dt_vvorset}], value(keyset, Map2)),

    ?assert(value({contains, {c, riak_dt_pncounter}}, Map2)),
    ?assertNot(value({contains, {l2, riak_dt_lwwreg}}, Map2)),

    Map3 = update({update, [{update, {c, riak_dt_pncounter}, {increment, 33}},
                            {update, {l, riak_dt_lwwreg}, {assign, lww_val, 77}}]}, a3, Map2),

    ?assertEqual(33, value({get, {c, riak_dt_pncounter}}, Map3)),
    ?assertEqual({lww_val, 77}, value({get_crdt, {l, riak_dt_lwwreg}}, Map3)).

-endif.
