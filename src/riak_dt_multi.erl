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
%% {name, type} -> CRDT value mappings.
%%
%% The value for a given field is tombstoned when the field is
%% removed. This map uses a reset / remove strategy to generate
%% tombstones. Reset attempts to set the value of the CRDT back to
%% bottom, without break monotonicity. For example, if a counter at
%% field F has a value of 10, then on remove of F, the counter is
%% decremented by 10, so its value is zero, and the field removed
%% from the keyset. A concurrent modification of that field on another
%% replica will result in the field being present in the Map, but its
%% value will be only that of the concurrent updates. This semantic
%% limits the types of CRDT that may be stored in the Map to those
%% that have a `reset/2' function. Currently the Map does not enforce
%% this. It would be nice if it did, or better still if it was
%% cognizant of a lattice of types so that an attempt to remove a
%% G-Counter would result in it being replaced to a
%% PN-Counter. Consider that work to be done.
%%
%% NOTE: a concurrent update at another node that appears to have no
%% effect, or an effect that is contained by the reset / remove
%% operation will still cause the field to be present. For example:
%% remove Set with members [a,b,c] from Field F at replica A.
%% Concurrently, remove member a from Set at Field F at replica B.
%% After merge, Field F is present with an empty Set. One proposed
%% future development is to not "count" updates concurrent with
%% removes that "contain" the update, again, work to be done.
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
         init_state/0, gen_field/0]).
-endif.

-export_type([multi/0, binary_multi/0, multi_op/0]).

-type binary_multi() :: binary(). %% A binary that from_binary/1 will accept
-type multi() :: {schema(), valuelist()}.
-type schema() :: riak_dt_vvorset:vvorset().
-type field() :: {Name::term(), Type::crdt_mod()}.
-type crdt_mod() :: riak_dt_pncounter | riak_dt_lwwreg |
                    riak_dt_orset | riak_dt_vvorset | riak_dt_od_flag |
                    riak_dt_multi.
-type valuelist() :: [{field(), crdt()}].

-type crdt()  ::  riak_dt_pncounter:pncounter() | riak_dt_od_flag:od_flag() |
                  riak_dt_lwwreg:lwwreg() |
                  riak_dt_vvorset:vvorset() | riak_dt_orset:orset() |
                  riak_dt_multi:multi().

-type multi_op() :: {update, [multi_field_update() | multi_field_op()]}.

-type multi_field_op() :: {add, field()} | {remove, field()}.
-type multi_field_update() :: {update, field(), crdt_op()}.

-type crdt_op() :: riak_dt_pncounter:pncounter_op() |
                   riak_dt_lwwreg:lwwreg_op() | riak_dt_orset:orset_op() |
                   riak_dt_vvorset:vvorset_op() | riak_dt_od_flag:od_flag_op() |
                   riak_dt_multi:multi_op().

-type multi_q() :: size | {get, field()} | {get_crdt, field()} |
                   keyset | {contains, field()}.

-type values() :: [value()].
-type value() :: {field(), riak_dt_multi:values() | integer() | [term()] | boolean() | term()}.
-type precondition_error() :: {error, {precondition, {not_present, field()}}}.

%% @doc Create a new, empty Map.
-spec new() -> multi().
new() ->
    {riak_dt_vvorset:new(), orddict:new()}.

%% @doc get the current set of values for this Map
-spec value(multi()) -> values().
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
%% the `multi_op()'. `Ops' is a list of one or more of the following
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
%% `{remove, `field()'}' where field is `{name, type}', results in
%% `reset/2' being called on the crdt at `field' and the key and value
%% being tombstoned.
%%
%% Either all of `Ops' are performed successfully, or none are.
-spec update(multi_op(), riak_dt:actor(), multi()) -> {ok, multi()} | precondition_error().
update({update, Ops}, Actor, {Schema, Values}) ->
    apply_ops(Ops, Actor, Schema, Values).

%% @Private
apply_ops([], _Actor, Schema, Values) ->
    {ok, {Schema, Values}};
apply_ops([{update, {_Name, Mod}=Key, Op} | Rest], Actor, Schema, Values) ->
    %% Add the key to schema
    {ok, NewSchema} = riak_dt_vvorset:update({add, Key}, Actor, Schema),
    %% Update the value
    InitialValue = fetch_with_default(Key, Values, Mod:new()),
    case Mod:update(Op, Actor, InitialValue) of
        {ok, NewValue} ->
            NewValues = orddict:store(Key, NewValue, Values),
            apply_ops(Rest, Actor, NewSchema, NewValues);
        Error ->
            Error
    end;
apply_ops([{remove, {_Name, Type}=Key} | Rest], Actor, Schema, Values) ->
    %% Do a reset remove, we keep the value as a tombstone, but we negate it
    case riak_dt_vvorset:update({remove, Key}, Actor, Schema) of
        {ok, NewSchema} ->
            case value({get_crdt, Key}, {Schema, Values}) of
                error ->
                    %% Already a tombstone
                    apply_ops(Rest, Actor, NewSchema, Values);
                CRDT ->
                    Tombstone = Type:reset(CRDT, Actor),
                    NewValues = orddict:store(Key, Tombstone, Values),
                    apply_ops(Rest, Actor, NewSchema, NewValues)
            end;
        _Error -> {error, {precondition, {not_present, Key}}}
    end;
apply_ops([{add, {_Name, Mod}=Key} | Rest], Actor, Schema, Values) ->
    {ok, NewSchema} = riak_dt_vvorset:update({add, Key}, Actor, Schema),
    NewValues = orddict:update(Key, fun(V) -> V end, Mod:new(), Values),
    apply_ops(Rest, Actor, NewSchema, NewValues).

fetch_with_default(Key, Values, Default) ->
    fetch_with_default(orddict:find(Key, Values), Default).

fetch_with_default({ok, Value}, _Default) ->
    Value;
fetch_with_default(error, Default) ->
    Default.

%% @Doc merge two `multi()'s.
%% Performs a merge on the key set (schema) and then a pairwise
%% merge on all values in the value list.
%% This is the LUB function.
-spec merge(multi(), multi()) -> multi().
merge({Schema1, Values1}, {Schema2, Values2}) ->
    NewSchema = riak_dt_vvorset:merge(Schema1, Schema2),
    NewValues = orddict:merge(fun({_Name, Mod}, V1, V2) ->
                                      Mod:merge(V1, V2) end,
                              Values1, Values2),
    {NewSchema, NewValues}.

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
reset([Field | Rest], Actor, Map0) ->
    %% No need to worry about the precondition error,
    %% Always called with the current key set
    %% @see reset/2
    {ok, Map} = update({update, [{remove, Field}]}, Actor, Map0),
    reset(Rest, Actor, Map).

%% @Doc a fragment of the Map that can be used for
%% precondition operations.
%% The schema is just the active Key Set
%% The values are just those values that are present
%% We use either the values precondition_context
%% or the whole CRDT
-spec precondition_context(multi()) -> multi().
precondition_context({KeySet0, Values0}) ->
    KeySet = riak_dt_vvorset:precondition_context(KeySet0),
    Present = riak_dt_vvorset:value(KeySet),
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
                %% I suspect a smarter tihng would be use nothing, or
                %% the value/1 value But that makes update more
                %% complex (the context would no longer be a CRDT)
    end.

-define(TAG, 77).
-define(V1_VERS, 1).

-spec to_binary(multi()) -> binary_multi().
to_binary(Map) ->
    %% @TODO something smarter (recurse down valulist calling to_binary?)
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Map))/binary>>.

-spec from_binary(binary_multi()) -> multi().
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
    {binary(), oneof([riak_dt_pncounter, riak_dt_vvorset,
                      riak_dt_lwwreg,
                      riak_dt_multi])}.

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
    PresentFields = [E ||  {E, _, _X} <- sets:to_list(sets:union(A,R))],
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

%% @doc get the current, or most recently tombstoned value for a given
%% key.
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
    case dict:find(K, Res) of
        error ->
            %% look for a tombstone, it will be the highest numbered remove
            R2 = lists:reverse(lists:keysort(3, sets:to_list(R))),
            case lists:keyfind(K, 1, R2) of
                false ->
                    T:new();
                {K, V, _Cnt} ->
                    %% reset
                    T:reset(V, ID)
            end;
        {ok, V} -> V
    end.

-endif.

query_test() ->
    Map = new(),
    {ok, Map1} = update({update, [{add, {c, riak_dt_pncounter}},
                                  {add, {s, riak_dt_vvorset}},
                                  {add, {m, riak_dt_multi}},
                                  {add, {l, riak_dt_lwwreg}},
                                  {add, {l2, riak_dt_lwwreg}}]}, a1, Map),
    ?assertEqual(5, value(size, Map1)),

    {ok, Map2} = update({update, [{remove, {l2, riak_dt_lwwreg}}]}, a2, Map1),
    ?assertEqual(4, value(size, Map2)),

    ?assertEqual([{c, riak_dt_pncounter},
                  {l, riak_dt_lwwreg},
                  {m, riak_dt_multi},
                  {s, riak_dt_vvorset}], value(keyset, Map2)),

    ?assert(value({contains, {c, riak_dt_pncounter}}, Map2)),
    ?assertNot(value({contains, {l2, riak_dt_lwwreg}}, Map2)),

    {ok, Map3} = update({update, [{update, {c, riak_dt_pncounter}, {increment, 33}},
                            {update, {l, riak_dt_lwwreg}, {assign, lww_val, 77}}]}, a3, Map2),

    ?assertEqual(33, value({get, {c, riak_dt_pncounter}}, Map3)),
    ?assertEqual({lww_val, 77}, value({get_crdt, {l, riak_dt_lwwreg}}, Map3)).

-endif.
