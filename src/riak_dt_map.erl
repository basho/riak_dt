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

%% @TODO refactor map to be a vclock and a dict of field-> [{dot, value}]
%% merge will simply keep sibling {field, dot, value}'s. Value will
%get all values for field and merge them.  5 update will gather all
%values for field, merge them, apply the op, and store with a new op
%(dropping all the old)

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
-type map() :: {riak_dt_vclock:vclock(), fields()}.
-type fields() :: orddict:orddict(field()).
-type field() :: {field_name(), [{dot(), crdt()}]}.
-type field_name() :: {Name::term(), Type::crdt_mod()}.
-type crdt_mod() :: riak_dt_pncounter | riak_dt_lwwreg |
                    riak_dt_od_flag |
                    riak_dt_map | riak_dt_orswot.

-type dot() :: {riak_dt:actor(), pos_integer()}.


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
    {riak_dt_vclock:fresh(), orddict:new()}.

%% @doc get the current set of values for this Map
-spec value(map()) -> values().
value({_Clock, Values}) ->
    orddict:fold(fun({_Name, Type}=Key, MemberInfo, Acc) ->
                         Merged = lists:foldl(fun({_Dot, Value}, Merged0) ->
                                                      Type:merge(Merged0, Value) end,
                                              Type:new(),
                                              MemberInfo),
                         [{Key, Type:value(Merged)} | Acc] end,
                 [],
                 Values).

%% @doc execute the given `map_q()' against the given
%% `map()'.
%% @TODO add a query for getting a subset of fields
%% including submap fields (Maybe kvc like?)
-spec value(map_q(), map()) -> term().
value(size, {_Clock, Dict}) ->
    length(keys(Dict));
value({get, {_Name, Mod}=Field}, Map) ->
    case value({get_crdt, Field}, Map) of
        error -> error;
        CRDT -> Mod:value(CRDT)
    end;
value({get_crdt, {_Name, Mod}=Field}, {_Clock, Values}) ->
    get_crdt(orddict:find(Field, Values), Mod);
value(keyset, {_Clock, Values}) ->
    keys(Values);
value({contains, Field}, {_Clock, Values}) ->
    lists:member(Field, keys(Values)).

get_crdt(CRDT, Type) ->
    get_crdt(CRDT, Type, false).

get_crdt({ok, Values}, Type, _) ->
    lists:foldl(fun({_Dot, Value}, Merged) ->
                        Type:merge(Merged, Value) end,
                Type:new(),
                Values);
get_crdt(error, _Type, false) ->
    error;
get_crdt(error, Type, true) ->
    Type:new().

keys(Values) ->
    orddict:fetch_keys(Values).

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
update({update, Ops}, Actor, {Clock, Values}) ->
    apply_ops(Ops, Actor, Clock, Values).

%% @Private
apply_ops([], _Actor, Clock, Values) ->
    {ok, {Clock, Values}};
apply_ops([{update, {_Name, Mod}=Field, Op} | Rest], Actor, Clock, Values) ->
    InitialValue = get_crdt(orddict:find(Field, Values), Mod, true),
    case Mod:update(Op, Actor, InitialValue) of
        {ok, NewValue} ->
            NewClock = riak_dt_vclock:increment(Actor, Clock),
            NewDot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
            NewValues = orddict:store(Field, [{NewDot, NewValue}], Values),
            apply_ops(Rest, Actor, NewClock, NewValues);
        Error ->
            Error
    end;
apply_ops([{remove, Field} | Rest], Actor, Clock, Values) ->
    case orddict:is_key(Field, Values) of
        true ->
            apply_ops(Rest, Actor, Clock, orddict:erase(Field, Values));
        false -> {error, {precondition, {not_present, Field}}}
    end;
apply_ops([{add, {_Name, Mod}=Field} | Rest], Actor, Clock, Values) ->
    InitialValue = get_crdt(orddict:find(Field, Values), Mod, true),
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
    NewValues = orddict:store(Field, [{Dot, InitialValue}], Values),
    apply_ops(Rest, Actor, NewClock, NewValues).

%% @Doc merge two `map()'s.  Performs a merge on the key set (schema)
%% and then a pairwise merge on all values in the value list.  This is
%% the LUB function.
-spec merge(map(), map()) -> map().
merge({LHSClock, LHSEntries}, {RHSClock, RHSEntries}) ->
    Clock = riak_dt_vclock:merge([LHSClock, RHSClock]),
    %% If an element is in both dicts, union {dot, value} siblings. If
    %% it occurs in one, then see if its dots are dominated by the
    %% others whole set clock. If so, then drop it, if not, keep it.
    LHSKeys = sets:from_list(orddict:fetch_keys(LHSEntries)),
    RHSKeys = sets:from_list(orddict:fetch_keys(RHSEntries)),
    CommonKeys = sets:intersection(LHSKeys, RHSKeys),
    LHSUnique = sets:subtract(LHSKeys, CommonKeys),
    RHSUnique = sets:subtract(RHSKeys, CommonKeys),

    Entries00 = merge_common_keys(CommonKeys, LHSEntries, RHSEntries),
    Entries0 = merge_disjoint_keys(LHSUnique, LHSEntries, RHSClock, Entries00),
    Entries = merge_disjoint_keys(RHSUnique, RHSEntries, LHSClock, Entries0),

    {Clock, Entries}.

%% @doc check if each element in `Entries' should be in the merged
%% set.
merge_disjoint_keys(Keys, Entries, SetClock, Accumulator) ->
    sets:fold(fun(Key, Acc) ->
                      Values = orddict:fetch(Key, Entries),
                      NewValues = lists:foldl(fun({Dot, Value}, DVs) ->
                                                      case riak_dt_vclock:descends(SetClock, [Dot]) of
                                                          false ->
                                                              [{Dot, Value} | DVs];
                                                          true ->
                                                              DVs
                                                      end
                                              end,
                                              [],
                                              Values),
                      case NewValues of
                          [] ->
                              Acc;
                          _ ->
                              orddict:store(Key, NewValues, Acc)
                      end
              end,
              Accumulator,
              Keys).

%% @doc keeps the {dot, value} pairs for all common entries
merge_common_keys(CommonKeys, Entries1, Entries2) ->
    sets:fold(fun(Key, Acc) ->
                      V1 = orddict:fetch(Key, Entries1),
                      V2 = orddict:fetch(Key, Entries2),
                      V = sets:union(sets:from_list(V1),
                                     sets:from_list(V2)),
                      orddict:store(Key, sets:to_list(V), Acc) end,
              orddict:new(),
              CommonKeys).

%% @Doc compare two `map()'s for equality of structure Both schemas
%% and value list must be equal. Performs a pariwise equals for all
%% values in the value lists
-spec equal(map(), map()) -> boolean().
equal({Clock1, Values1}, {Clock2, Values2}) ->
    riak_dt_vclock:equal(Clock1, Clock2) andalso
        orddict:fetch_keys(Values1) == orddict:fetch_keys(Values2) andalso
        pairwise_equals(Values1, Values2).

%% @Private Note, only called when we know that both dicts have the
%% same set of keys.
pairwise_equals(Values1, Values2) ->
    short_cicuit_equals(orddict:to_list(Values1), Values2).

%% @Private Compare each value. Return false as soon as any pair are
%% not equal.
short_cicuit_equals([], _Values2) ->
    true;
short_cicuit_equals([{{_Name, _Type}=Key, Val1} | Rest], Values2) ->
    Val2 = orddict:fetch(Key, Values2),
    case lists:sort(Val1) == lists:sort(Val2) of
        true ->
            short_cicuit_equals(Rest, Values2);
        false ->
            false
    end.

%% @Doc a "fragment" of the Map that can be used for precondition
%% operations.
-spec precondition_context(map()) -> map().
precondition_context(Map) ->
    Map.

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
