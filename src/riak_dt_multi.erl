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
-export([new/0, value/1, update/3, merge/2, equal/2, to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0]).
-endif.

-opaque multi() :: {schema(), valuelist()}.
-type schema() :: riak_dt_vvorset:vvorset().
-type field() :: {Name::term(), Type::crdt_mod()}.
-type crdt_mod() :: module(). %% @TODO add them all?
-type valuelist() :: [{field(), Value::term()}]. %% @TODO add all the crdt types?

-spec new() -> multi().
new() ->
    {riak_dt_vvorset:new(), orddict:new()}.

%% @doc get the current set of values for this multi
%% @TODO a way to get the value of a subset of fields.
value({Schema, Values}) ->
    PresentFields = riak_dt_vvorset:value(Schema),
    values(PresentFields, Values).

%% private
values(PresentFields, Values) ->
    values(PresentFields, Values, []).

values([], _Values, Acc) ->
    Acc;
values([{_Name, Mod}=Key | Rest], Values, Acc) ->
    CRDT = orddict:fetch(Key, Values),
    Val = Mod:value(CRDT),
    values(Rest, Values, [{Key, Val} | Acc]).

%% @Doc update the schema or a field in the schema.
%% Ops is a list of one or more of the following ops:
%% {update, Key, Op} where Op is any operation supported by the CRDT mod
%% referenced in the Key ({name, type}). The Op is applied to the value in the value list
%% for the given key
%% {add, field} where field is {name, type}.
%% {remove, field} where field is {name, type}
%% These last too are just the OR-Set operations add/remove on the schema.
update({update, Ops}, Actor, {Schema, Values}) ->
    apply_ops(Ops, Actor, Schema, Values).

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
apply_ops([{remove, Key} | Rest], Actor, Schema, Values) ->
    NewSchema = riak_dt_vvorset:update({remove, Key}, Actor, Schema),
    NewValues = orddict:erase(Key, Values),
    apply_ops(Rest, Actor, NewSchema, NewValues);
apply_ops([{add, {_Name, Mod}=Key} | Rest], Actor, Schema, Values) ->
    %% Add the key to schema
    NewSchema = riak_dt_vvorset:update({add, Key}, Actor, Schema),
    %% Update the value, adding an empty crdt if not present
    NewValues = orddict:update(Key, fun(V) -> V end, Mod:new(), Values),
    apply_ops(Rest, Actor, NewSchema, NewValues).

update_fun(Mod, Op, Actor) ->
    fun(CRDT) ->
            Mod:update(Op, Actor, CRDT)
    end.

merge({Schema1, Values1}, {Schema2, Values2}) ->
    NewSchema = riak_dt_vvorset:merge(Schema1, Schema2),
    PresentFields = riak_dt_vvorset:value(NewSchema),
    NewValues = merge_values(PresentFields, Values1, Values2, []),
    %% This merges _all_ values in the value list.
    %% Is that a good idea? It means merging items that maybe (long ago?!) deleted
    %% However, maybe that is good in case they get re-added.
    %% @TODO Discuss this vs the above
    %% NewValues = orddict:merge(fun({_Name, Type}, V1, V2) -> Type:merge(V1, V2) end,
    %%                           Values1, Values2),
    {NewSchema, NewValues}.

merge_values([], _Values1, _Values2, Acc) ->
    orddict:from_list(Acc);
merge_values([{_Name, Mod}=Key | Rest], Values1, Values2, Acc) ->
    V1 = type_safe_fetch(Mod, orddict:find(Key, Values1)),
    V2 = type_safe_fetch(Mod, orddict:find(Key, Values2)),
    V = Mod:merge(V1, V2),
    merge_values(Rest, Values1, Values2, [{Key, V} | Acc]).

type_safe_fetch(_Mod, {ok, Value}) ->
    Value;
type_safe_fetch(Mod, error) ->
    Mod:new().

%% @TODO compare equals on all present fields or all fields??
%% NOTE: this fun is (probably) incorrect
equal({Schema1, Values1}, {Schema2, Values2}) ->
    riak_dt_vvorset:equal(Schema1, Schema2)  andalso pairwise_equals(Values1, Values2).

%% Note, only called when we know that 2 schema are equal.
%% Both dicts therefore have the same set of keys.
pairwise_equals(Values1, Values2) ->
    short_cicuit_equals(orddict:to_list(Values1), Values2).

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
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(init_state(), 1000, ?MODULE))]}.

%% EQC generator
gen_op() ->
    ?LET(Ops, non_empty(list(gen_update())), {update, Ops}).

gen_update() ->
    ?LET(Field, gen_field(),
         oneof([{add, Field}, {remove, Field}, {update, Field, gen_field_op(Field)}])).

gen_field() ->
    {binary(), oneof([riak_dt_pncounter, riak_dt_vvorset, riak_dt_lwwreg, riak_dt_multi, riak_dt_gcounter])}.

gen_field_op({_Name, Type}) ->
    Type:gen_op().

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
-endif.
