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
         equal/2, to_binary/1, from_binary/1, precondition_context/1, stats/1, stat/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1,
         init_state/0, gen_field/0, generate/0, size/1]).
-endif.

-export_type([map/0, binary_map/0, map_op/0]).

-type binary_map() :: binary(). %% A binary that from_binary/1 will accept
-type map() :: {riak_dt_vclock:vclock(), valuelist()}.
-type field() :: {Name::term(), Type::crdt_mod()}.
-type crdt_mod() :: riak_dt_pncounter | riak_dt_lwwreg |
                    riak_dt_od_flag |
                    riak_dt_map | riak_dt_orswot.
-type valuelist() :: [{field(), entry()}].
-type entry() :: {minimal_clock(), crdt()}.

%% a minimal clock is just the dots for the element, each dot being an
%% actor and event counter for when the element was added.
-type minimal_clock() :: [dot()].
-type dot() :: {riak_dt:actor(), Count::pos_integer()}.

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
    orddict:fold(fun({_Name, Mod}=Key, {_Dots, Value}, Acc) ->
                       [{Key, Mod:value(Value)} | Acc] end,
                 [],
                 Values).

%% @doc execute the given `map_q()' against the given
%% `map()'.
%% @TODO add a query for getting a subset of fields
%% including submap fields (Maybe kvc like?)
-spec value(map_q(), map()) -> term().
value(size, {_Clock, Values}) ->
    length(keys(Values));
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

%% @private
-spec get_crdt({ok, entry()} | error, crdt_mod()) -> crdt() | error.
get_crdt(Entry, Mod) ->
    get_crdt(Entry, Mod, false).

%% @private
-spec get_crdt({ok, entry()} | error, crdt_mod(), boolean()) ->
                      crdt() | error.
get_crdt({ok, {_Dot, Value}}, _Mod, _) ->
    Value;
get_crdt(error, _Mod, false) ->
    error;
get_crdt(error, Mod, true) ->
    Mod:new().

-spec keys(orddict:orddict()) -> [field()] | [].
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

%% @private
-spec apply_ops([map_field_update() | map_field_op()], riak_dt:actor(),
                riak_dt_vclock:vclock(), orddict:orddict()) ->
                       {ok, map()} | precondition_error().
apply_ops([], _Actor, Clock, Values) ->
    {ok, {Clock, Values}};
apply_ops([{update, {_Name, Mod}=Field, Op} | Rest], Actor, Clock, Values) ->
    InitialValue = get_crdt(orddict:find(Field, Values), Mod, true),
    case Mod:update(Op, Actor, InitialValue) of
        {ok, NewValue} ->
            NewClock = riak_dt_vclock:increment(Actor, Clock),
            NewDot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
            NewValues = orddict:store(Field, {[NewDot], NewValue}, Values),
            apply_ops(Rest, Actor, NewClock, NewValues);
        Error ->
            Error
    end;
apply_ops([{remove, Field} | Rest], Actor, Clock, Values) ->
    case orddict:is_key(Field, Values) of
        true->
            apply_ops(Rest, Actor, Clock, orddict:erase(Field, Values));
        false -> {error, {precondition, {not_present, Field}}}
    end;
apply_ops([{add, {_Name, Mod}=Field} | Rest], Actor, Clock, Values) ->
    InitialValue = get_crdt(orddict:find(Field, Values), Mod, true),
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
    NewValues = orddict:store(Field, {[Dot], InitialValue}, Values),
    apply_ops(Rest, Actor, NewClock, NewValues).

%% @Doc merge two `map()'s.  and then a pairwise merge on all values
%% in the value list.  This is the LUB function.
-spec merge(map(), map()) -> map().
merge({LHSClock, LHSEntries}, {RHSClock, RHSEntries}) ->
    Clock = riak_dt_vclock:merge([LHSClock, RHSClock]),
    %% If an element is in both dicts, merge it. If it occurs in one,
    %% then see if its dots are dominated by the others whole set
    %% clock. If so, then drop it, if not, keep it.
    LHSFields = sets:from_list(orddict:fetch_keys(LHSEntries)),
    RHSFields = sets:from_list(orddict:fetch_keys(RHSEntries)),
    CommonFields = sets:intersection(LHSFields, RHSFields),
    LHSUnique = sets:subtract(LHSFields, CommonFields),
    RHSUnique = sets:subtract(RHSFields, CommonFields),

    Entries00 = merge_common_fields(CommonFields, LHSEntries, RHSEntries),
    Entries0 = merge_disjoint_fields(LHSUnique, LHSEntries, RHSClock, Entries00),
    Entries = merge_disjoint_fields(RHSUnique, RHSEntries, LHSClock, Entries0),

    {Clock, Entries}.

%% @doc check if each element in `Entries' should be in the merged
%% set.
-spec merge_disjoint_fields(set(), valuelist(),
                            riak_dt_vclock:vclock(), valuelist()) ->
                                   valuelist().
merge_disjoint_fields(Fields, Entries, SetClock, Accumulator) ->
    sets:fold(fun(Field, Acc) ->
                      {Dots, Value} = orddict:fetch(Field, Entries),
                      case riak_dt_vclock:descends(SetClock, Dots) of
                          false ->
                              %% Optimise the set of stored dots to
                              %% include only those unseen
                              NewDots = riak_dt_vclock:subtract_dots(Dots, SetClock),
                              orddict:store(Field, {NewDots, Value}, Acc);
                          true ->
                              Acc
                      end
              end,
              Accumulator,
              Fields).

%% @doc merges the minimal clocks and values for the common entries in
%% both sets.
-spec merge_common_fields(set(), valuelist(), valuelist()) -> valuelist().
merge_common_fields(CommonFields, Entries1, Entries2) ->
    sets:fold(fun({_Name, Mod}=Field, Acc) ->
                      {Dots1, V1} = orddict:fetch(Field, Entries1),
                      {Dots2, V2} = orddict:fetch(Field, Entries2),
                      Dots = riak_dt_vclock:merge([Dots1, Dots2]),
                      V = Mod:merge(V1, V2),
                      orddict:store(Field, {Dots, V}, Acc) end,
              orddict:new(),
              CommonFields).

%% @Doc compare two `map()'s for equality of structure Both schemas
%% and value list must be equal. Performs a pariwise equals for all
%% values in the value lists
-spec equal(map(), map()) -> boolean().
equal({Clock1, Values1}, {Clock2, Values2}) ->
    riak_dt_vclock:equal(Clock1, Clock2) andalso
        keys(Values1) == keys(Values2) andalso
        pairwise_equals(Values1, Values2).

%% @Private Note, only called when we know that 2 sets of fields are
%% equal. Both dicts therefore have the same set of keys.
-spec pairwise_equals(valuelist(), valuelist()) -> boolean().
pairwise_equals(Values1, Values2) ->
    short_circuit_equals(orddict:to_list(Values1), Values2).

%% @Private
%% Compare each value. Return false as soon as any pair are not equal.
-spec short_circuit_equals(valuelist(), valuelist()) -> boolean().
short_circuit_equals([], _Values2) ->
    true;
short_circuit_equals([{{_Name, Mod}=Field, {Dot1,Val1}} | Rest], Values2) ->
    {Dot2, Val2} = orddict:fetch(Field, Values2),
    case {riak_dt_vclock:equal(Dot1, Dot2), Mod:equal(Val1, Val2)} of
        {true, true} ->
            short_circuit_equals(Rest, Values2);
        _ ->
            false
    end.

%% @Doc a "fragment" of the Map that can be used for precondition
%% operations. The schema is just the active Key Set The values are
%% just those values that are present We use either the values
%% precondition_context or the whole CRDT
-spec precondition_context(map()) -> map().
precondition_context(Map) ->
    Map.

-spec stats(map()) -> [{atom(), integer()}].
stats(Map) ->
    [ {S, stat(S, Map)} || S <- [actor_count, field_count, max_dot_length]].

-spec stat(atom(), map()) -> number() | undefined.
stat(actor_count, {Clock, _}) ->
    length(Clock);
stat(field_count, {_, Fields}) ->
    length(Fields);
stat(max_dot_length, {_, Fields}) ->
    orddict:fold(fun(_K, {Dots, _}, Acc) ->
                         max(length(Dots), Acc)
                 end, 0, Fields);
stat(_,_) -> undefined.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_MAP_TAG).
-define(V1_VERS, 1).

%% @doc returns a binary representation of the provided `map()'. The
%% resulting binary is tagged and versioned for ease of future
%% upgrade. Calling `from_binary/1' with the result of this function
%% will return the original map.  Use the application env var
%% `binary_compression' to turn t2b compression on (`true') and off
%% (`false')
%%
%% @see `from_binary/1'
-spec to_binary(map()) -> binary_map().
to_binary(Map) ->
    Opts = case application:get_env(riak_dt, binary_compression, 1) of
               true -> [compressed];
               N when N >= 0, N =< 9 -> [{compressed, N}];
               _ -> []
           end,
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Map, Opts))/binary>>.

%% @doc When the argument is a `binary_map()' produced by
%% `to_binary/1' will return the original `map()'.
%%
%% @see `to_binary/1'
-spec from_binary(binary_map()) -> map().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    binary_to_term(B).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
-define(NUMTESTS, 1000).

bin_roundtrip_test_() ->
    crdt_statem_eqc:run_binary_rt(?MODULE, ?NUMTESTS).

eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, ?NUMTESTS).

%% ===================================
%% crdt_statem_eqc callbacks
%% ===================================
size(Map) ->
    byte_size(term_to_binary(Map)) div 10.

generate() ->
        ?LET({Ops, Actors}, {non_empty(list(gen_op())), non_empty(list(bitstring(16*8)))},
         lists:foldl(fun(Op, Map) ->
                             Actor = case length(Actors) of
                                         1 -> hd(Actors);
                                         _ -> lists:nth(crypto:rand_uniform(1, length(Actors)), Actors)
                                     end,
                             case riak_dt_map:update(Op, Actor, Map) of
                                 {ok, M} -> M;
                                 _ -> Map
                             end
                     end,
                     riak_dt_map:new(),
                     Ops)).


gen_op() ->
    ?LET(Ops, non_empty(list(gen_update())), {update, Ops}).

gen_update() ->
    ?LET(Field, gen_field(),
         oneof([{add, Field}, {remove, Field},
                {update, Field, gen_field_op(Field)}])).

gen_field() ->
    {non_empty(binary()), oneof([riak_dt_pncounter, riak_dt_orswot,
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


stat_test() ->
    Map = new(),
    {ok, Map1} = update({update, [{add, {c, riak_dt_pncounter}},
                                  {add, {s, riak_dt_orswot}},
                                  {add, {m, riak_dt_map}},
                                  {add, {l, riak_dt_lwwreg}},
                                  {add, {l2, riak_dt_lwwreg}}]}, a1, Map),
    {ok, Map2} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"foo">>, 1}}]}, a2, Map1),
    {ok, Map3} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"bar">>, 2}}]}, a3, Map1),
    Map4 = merge(Map2, Map3),
    ?assertEqual([{actor_count, 0}, {field_count, 0}, {max_dot_length, 0}], stats(Map)),
    ?assertEqual(3, stat(actor_count, Map4)),
    ?assertEqual(5, stat(field_count, Map4)),
    ?assertEqual(2, stat(max_dot_length, Map4)),
    ?assertEqual(undefined, stat(waste_pct, Map4)).

-endif.
