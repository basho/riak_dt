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

-include("riak_dt_backend_impl.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, update/3, merge/2,
         equal/2, to_binary/1, from_binary/1, precondition_context/1, stats/1]).

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

%% Used by to/from binary to only store mod names once
-type mod_map() :: [{crdt_mod(), pos_integer()}].

%% Used by to/from binary to only store each actor id once
-type actor_map() :: [{riak_dt:actor(), pos_integer()}
                      | {pos_integer(), riak_dt:actor()}].

%% @doc Create a new, empty Map.
-spec new() -> map().
new() ->
    {riak_dt_vclock:fresh(), ?DT_ERL_DICT:new()}.

%% @doc get the current set of values for this Map
-spec value(map()) -> values().
value({_Clock, Values}) ->
    ?DT_ERL_DICT:fold(fun({_Name, Mod}=Key, {_Dots, Value}, Acc) ->
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
    get_crdt(?DT_ERL_DICT:find(Field, Values), Mod);
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
    ?DT_ERL_DICT:fetch_keys(Values).

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
    InitialValue = get_crdt(?DT_ERL_DICT:find(Field, Values), Mod, true),
    case Mod:update(Op, Actor, InitialValue) of
        {ok, NewValue} ->
            NewClock = riak_dt_vclock:increment(Actor, Clock),
            NewDot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
            NewValues = ?DT_ERL_DICT:store(Field, {[NewDot], NewValue}, Values),
            apply_ops(Rest, Actor, NewClock, NewValues);
        Error ->
            Error
    end;
apply_ops([{remove, Field} | Rest], Actor, Clock, Values) ->
    case ?DT_ERL_DICT:is_key(Field, Values) of
        true->
            apply_ops(Rest, Actor, Clock, ?DT_ERL_DICT:erase(Field, Values));
        false -> {error, {precondition, {not_present, Field}}}
    end;
apply_ops([{add, {_Name, Mod}=Field} | Rest], Actor, Clock, Values) ->
    InitialValue = get_crdt(?DT_ERL_DICT:find(Field, Values), Mod, true),
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
    NewValues = ?DT_ERL_DICT:store(Field, {[Dot], InitialValue}, Values),
    apply_ops(Rest, Actor, NewClock, NewValues).

%% @Doc merge two `map()'s.  and then a pairwise merge on all values
%% in the value list.  This is the LUB function.
-spec merge(map(), map()) -> map().
merge({LHSClock, LHSEntries}, {RHSClock, RHSEntries}) ->
    Clock = riak_dt_vclock:merge([LHSClock, RHSClock]),
    %% If an element is in both dicts, merge it. If it occurs in one,
    %% then see if its dots are dominated by the others whole set
    %% clock. If so, then drop it, if not, keep it.
    LHSFields = ?DT_ERL_SETS:from_list(?DT_ERL_DICT:fetch_keys(LHSEntries)),
    RHSFields = ?DT_ERL_SETS:from_list(?DT_ERL_DICT:fetch_keys(RHSEntries)),
    CommonFields = ?DT_ERL_SETS:intersection(LHSFields, RHSFields),
    LHSUnique = ?DT_ERL_SETS:subtract(LHSFields, CommonFields),
    RHSUnique = ?DT_ERL_SETS:subtract(RHSFields, CommonFields),

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
                      {Dots, Value} = ?DT_ERL_DICT:fetch(Field, Entries),
                      case riak_dt_vclock:descends(SetClock, Dots) of
                          false ->
                              %% Optimise the set of stored dots to
                              %% include only those unseen
                              NewDots = riak_dt_vclock:subtract_dots(Dots, SetClock),
                              ?DT_ERL_SETS:store(Field, {NewDots, Value}, Acc);
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
                      {Dots1, V1} = ?DT_ERL_DICT:fetch(Field, Entries1),
                      {Dots2, V2} = ?DT_ERL_DICT:fetch(Field, Entries2),
                      Dots = riak_dt_vclock:merge([Dots1, Dots2]),
                      V = Mod:merge(V1, V2),
                      ?DT_ERL_DICT:store(Field, {Dots, V}, Acc) end,
              ?DT_ERL_DICT:new(),
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
    short_circuit_equals(?DT_ERL_DICT:to_list(Values1), Values2).

%% @Private
%% Compare each value. Return false as soon as any pair are not equal.
-spec short_circuit_equals(valuelist(), valuelist()) -> boolean().
short_circuit_equals([], _Values2) ->
    true;
short_circuit_equals([{{_Name, Mod}=Field, {Dot1,Val1}} | Rest], Values2) ->
    {Dot2, Val2} = ?DT_ERL_DICT:fetch(Field, Values2),
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
stats({Clock, Fields}) ->
    [
     {actor_count, length(Clock)},
     {field_count, ?DT_ERL_DICT:size(Fields)},
     {max_dot_length,
      ?DT_ERL_DICT:fold(fun(_K, {Dots, _}, Acc) ->
                           max(length(Dots), Acc)
                   end, 0, Fields)}
    ].


-define(TAG, 77).
-define(V1_VERS, 1).

%% @doc returns a binary representation of the provided
%% `map()'. Measurements show that for all but the empty map this
%% function is more effecient than using
%% `erlang:term_to_binary/1'. The resulting binary is tagged and
%% versioned for ease of future upgrade. Calling `from_binary/1' with
%% the result of this function will return the original map.
%%
%% @see `from_binary/1'
-spec to_binary(map()) -> binary_map().
to_binary(Map) ->
    {ModMap, <<?TAG:8/integer, ?V1_VERS:8/integer, MapBin/binary>>} =  to_binary(Map, ?DT_ERL_DICT:new()),
    ModMapBin = mod_map_to_binary(ModMap),
    ModMapBinLen = byte_size(ModMapBin),
    <<?TAG:8/integer, ?V1_VERS:8/integer,
      ModMapBinLen:32/integer,
      ModMapBin:ModMapBinLen/binary,
      MapBin/binary>>.

%% @doc When the argument is a `binary_map()' produced by
%% `to_binary/1' will return the original `map()'.
%%
%% @see `to_binary/1'
-spec from_binary(binary_map()) -> map().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer,
              ModMapBinLen:32/integer, ModMapBin:ModMapBinLen/binary,
              MapBin/binary>>) ->
    ModMap = mod_map_from_binary(ModMapBin),
    from_binary(MapBin, ModMap).

%% @private internal `to_binary/2' for submaps. Threads the `ModMap'
%% through all submaps so that we don't duplicate information.
-spec to_binary(map(), mod_map()) -> {mod_map(), binary_map()}.
to_binary({Clock, Fields}, ModMap0) ->
    Actors0 = riak_dt_vclock:all_nodes(Clock),
    Actors = lists:zip(Actors0, lists:seq(1, length(Actors0))),
    BinClock = riak_dt_vclock:to_binary(Clock),
    ClockLen = byte_size(BinClock),
    %% since each minimal clock can be at _most_ the size of the
    %% vclock for the set, this saves us reserving 4 bytes for clock
    %% len for each entry if we only needs 1 (for example)
    ClockLenFieldLen = bit_size(binary:encode_unsigned(ClockLen)),
    {ModMap, BinFields} = fields_to_binary(Fields, ClockLenFieldLen, Actors, ModMap0, <<>>),
    {ModMap, <<?TAG:8/integer, ?V1_VERS:8/integer,
      ClockLenFieldLen:8/integer,
      ClockLen:ClockLenFieldLen/integer,
      BinClock:ClockLen/binary,
      BinFields/binary>>}.

%% @private internal from binary that threads the `ModMap' for use by
%% all submaps.
-spec from_binary(binary(), mod_map()) -> map().
from_binary(<<ClockLenFieldLen:8/integer, ClockLen:ClockLenFieldLen/integer,
              BinClock:ClockLen/binary, BinFields/binary>>, ModMap) ->
    Clock = riak_dt_vclock:from_binary(BinClock),
    Actors0 = riak_dt_vclock:all_nodes(Clock),
    Actors = lists:zip(lists:seq(1, length(Actors0)), Actors0),
    Fields = binary_to_fields(BinFields, ClockLenFieldLen, Actors, ModMap, ?DT_ERL_DICT:new()),
    {Clock, Fields}.

%% @private inverse of 'binary_to_fields/5'
-spec fields_to_binary(valuelist(), pos_integer(), actor_map(),
                       mod_map(), binary()) ->
                              {mod_map(), binary()}.
fields_to_binary([], _, _, ModMap, BinFields) ->
    {ModMap, BinFields};
fields_to_binary([{Field, {Clock0, CRDT}} | Rest], ClockLenFieldLen, Actors, ModMap0, Acc) ->
    {ModMap, CRDTBin} = crdt_to_binary(Field, CRDT, ModMap0),
    CRDTBinLen = byte_size(CRDTBin),
    Clock = riak_dt_vclock:replace_actors(Actors, Clock0),
    ClockBin = riak_dt_vclock:to_binary(Clock),
    ClockLen = byte_size(ClockBin),
    Bin = <<CRDTBinLen:32/integer, CRDTBin:CRDTBinLen/binary,
            ClockLen:ClockLenFieldLen/integer, ClockBin:ClockLen/binary>>,
    fields_to_binary(Rest, ClockLenFieldLen, Actors, ModMap, <<Acc/binary, Bin/binary>>).

%% @private inverse of 'binary_to_crdt/2'
-spec crdt_to_binary(field(), crdt(), mod_map()) -> {mod_map(), binary()}.
crdt_to_binary({Name, Mod}, CRDT, ModMap0) ->
    NameBin = name_to_bin(Name),
    NameBinLen = byte_size(NameBin),
    {ModMapping, ModMap} = mod_mapping(Mod, ModMap0),
    {ModMap2, ValueBin} = value_to_binary(Mod, CRDT, ModMap),
    ValueBinLen = byte_size(ValueBin),
    Bin = <<ModMapping:8/integer,
            NameBinLen:32/integer,
            NameBin:NameBinLen/binary,
            ValueBinLen:32/integer,
            ValueBin:ValueBinLen/binary>>,
    {ModMap2, Bin}.

%% @private inverse of `mod_map_from_binary/1'
-spec mod_map_to_binary(mod_map()) -> binary().
mod_map_to_binary(ModMap) ->
    term_to_binary(ModMap).

%% @private inverse of `binary_to_name/1'
-spec name_to_bin(term()) -> binary().
name_to_bin(Name) when is_binary(Name) ->
    <<1, Name/binary>>;
name_to_bin(Name) ->
    <<0, (term_to_binary(Name))/binary>>.

%% @private inverse of `binary_to_value/3'
-spec value_to_binary(crdt_mod(), crdt(), mod_map()) ->
                             {mod_map(), binary()}.
value_to_binary(?MODULE, Value, ModMap) ->
    to_binary(Value, ModMap);
value_to_binary(Mod, Value, ModMap) ->
    {ModMap, Mod:to_binary(Value)}.

%% @private get or generate an int -> mod mapping
-spec mod_mapping(crdt_mod(), mod_map()) -> {pos_integer(), mod_map()}.
mod_mapping(Mod, ModMap0) ->
    case ?DT_ERL_DICT:find(Mod, ModMap0) of
        error ->
            NewMapping = ?DT_ERL_DICT:size(ModMap0) + 1,
            {NewMapping, ?DT_ERL_DICT:store(Mod, NewMapping, ModMap0)};
        {ok, Mapping} ->
            {Mapping, ModMap0}
    end.

%% @private inverse of `fields_to_binary/5'
-spec binary_to_fields(binary(), pos_integer(),
                       actor_map(), mod_map(), valuelist()) ->
                              valuelist().
binary_to_fields(<<>>, _, _, _, Fields) ->
    Fields;
binary_to_fields(<<CRDTBinLen:32/integer, CRDTBin:CRDTBinLen/binary, Rest0/binary>>,
                 ClockLenFieldLen, Actors, ModMap, Acc) ->
    <<ClockLen:ClockLenFieldLen/integer, ClockBin:ClockLen/binary, Rest/binary>> = Rest0,
    {Field, CRDT} = binary_to_crdt(CRDTBin, ModMap),
    Clock0 = riak_dt_vclock:from_binary(ClockBin),
    Clock = riak_dt_vclock:replace_actors(Actors, Clock0),
    Fields = ?DT_ERL_DICT:store(Field, {Clock, CRDT}, Acc),
    binary_to_fields(Rest, ClockLenFieldLen, Actors, ModMap, Fields).

%% @private invers of `crdt_to_binary/3'
-spec binary_to_crdt(binary(), mod_map()) ->
                            {field(), crdt()}.
binary_to_crdt(<<ModMapping:8/integer,
            NameBinLen:32/integer,
            NameBin:NameBinLen/binary,
            ValueBinLen:32/integer,
            ValueBin:ValueBinLen/binary>>, ModMap) ->
    Mod = mod_from_mapping(ModMapping, ModMap),
    Name = binary_to_name(NameBin),
    Value = binary_to_value(ValueBin, Mod, ModMap),
    {{Name, Mod}, Value}.

%% @private inverse of `mod_mapping/2', get the mod for and int
%% mapping.
-spec mod_from_mapping(pos_integer(), mod_map()) -> crdt_mod().
mod_from_mapping(ModMapping, ModMap) ->
    {Mod, ModMapping} = lists:keyfind(ModMapping, 2, ModMap),
    Mod.

%% @private inverse of `name_to_bin/1'
-spec binary_to_name(binary()) -> term().
binary_to_name(<<1, Bin/binary>>) ->
    Bin;
binary_to_name(<<0, Bin/binary>>) ->
    binary_to_term(Bin).

%% @private inverse of `binary_to_value/4'
-spec binary_to_value(binary(), crdt_mod(), mod_map()) -> crdt().
binary_to_value(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>, ?MODULE, ModMap) ->
    from_binary(Bin, ModMap);
binary_to_value(Bin, Mod, _) ->
    Mod:from_binary(Bin).

%% @private inverse of `mod_map_to_binary/1'
-spec mod_map_from_binary(binary()) -> mod_map().
mod_map_from_binary(ModMapBin) ->
    binary_to_term(ModMapBin).

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

-endif.
