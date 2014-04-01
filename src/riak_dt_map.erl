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

%% @doc a multi CRDT holder. A Struct/Document-ish thing. Uses the
%% same tombstone-less, Observed Remove semantics as `riak_dt_orswot'.
%% A Map is set of `Field's a `Field' is a two-tuple of:
%% `{Name::binary(), CRDTModule::module()}' where the second element
%% is the name of a crdt module that conforms to the `riak_dt'
%% behaviour. CRDTs stored inside the Map will have their `update/3'
%% function called, but the second argument will be a `riak_dt:dot()',
%% so that they share the causal context of the map, even when fields
%% are removed, and subsequently re-added.
%%
%% The contents of the Map are modelled as a set of `{field(),
%% value(), dot()}' tuples, where `dot()' is the last event that
%% occurred on the field. When merging fields of the same name, but
%% different `dot' are _not_ merged. On updating a field, all the
%% elements in the set for that field are merged, updated, and
%% replaced with a new `dot' for the update event. This means that in
%% a divergent Map with many concurrent updates, a merged map will
%% have duplicate entries for any updated fields until an update event
%% occurs for that field, unifying the divergent field into a single
%% field.
%%
%% Attempting to remove a `field' that is not present in the map will
%% lead to a precondition error. An operation on a field value that
%% generates a precondition error will cause the Map operation to
%% return a precondition error. See `update/3' for details on
%% operations the Map accepts.
%%
%% @see riak_dt_orswot for more on the OR semantic
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
-export([new/0, value/1, value/2, update/3, update/4]).
-export([merge/2, equal/2, to_binary/1, from_binary/1]).
-export([precondition_context/1, stats/1, stat/2]).
-export([parent_clock/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1,
         init_state/0, gen_field/0, generate/0, size/1]).
-endif.

-export_type([map/0, binary_map/0, map_op/0]).

-type binary_map() :: binary(). %% A binary that from_binary/1 will accept
-type map() :: {riak_dt_vclock:vclock(), entries(), deferred()}.
-type entries() :: ordsets:ordset(entry()).
-type entry() :: {field_tag(), CRDT :: crdt()}.
-type field_tag() :: {Field :: field(), Tag :: riak_dt:dot()}.
-type field() :: {Name :: binary(), CRDTModule :: crdt_mod()}.
%% Only field removals can be deferred. CRDTs stored in the map may
%% have contexts and deferred operatiosn, but as these are part of the
%% state, they are stored under the field as an update like any other.
-type deferred() :: [{context(), [field()]}].

-type crdt_mod() :: riak_dt_pncounter | riak_dt_lwwreg |
                    riak_dt_od_flag |
                    riak_dt_map | riak_dt_orswot.

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

-type context() :: riak_dt_vclock:vclock() | undefined.

-type values() :: [value()].
-type value() :: {field(), riak_dt_map:values() | integer() | [term()] | boolean() | term()}.
-type precondition_error() :: {error, {precondition, {not_present, field()}}}.

%% @doc Create a new, empty Map.
-spec new() -> map().
new() ->
    {riak_dt_vclock:fresh(), ordsets:new(), orddict:new()}.

%% @doc sets the clock in the map to that `Clock'. Used by a
%% containing Map for sub-CRDTs
-spec parent_clock(riak_dt_vclock:vclock(), map()) -> map().
parent_clock(Clock, {_MapClock, Values, Deferred}) ->
    {Clock, Values, Deferred}.

%% @doc get the current set of values for this Map
-spec value(map()) -> values().
value({_Clock, Values, _Deferred}) ->
    Res = lists:foldl(fun({{{_Name, Type}=Key, _Dot}, Value}, Acc) ->
                              %% if key is in Acc merge with it and replace
                              dict:update(Key, fun(V) ->
                                                       Type:merge(V, Value) end,
                                          Value, Acc) end,
                      dict:new(),
                      Values),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res)].

%% @doc query map (not implemented yet)
-spec value(term(), map()) -> values().
value(_, Map) ->
    value(Map).

%% @doc update the `map()' or a field in the `map()' by executing
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
-spec update(map_op(), riak_dt:actor() | riak_dt:dot(), map()) ->
                    {ok, map()} | precondition_error().
update(Op, ActorOrDot, Map) ->
    update(Op, ActorOrDot, Map, undefined).

%% @doc the same as `update/3' except that the context ensures no
%% unseen field adds are removed, and removal of unseen adds is
%% deferred. The Context is passed on as the context for any nested
%% types.
-spec update(map_op(), riak_dt:actor() | riak_dt:dot(), map(), riak_dt:context()) ->
                    {ok, map()}.
update({update, Ops}, ActorOrDot, {Clock0, Values, Deferred}, Ctx) ->
    {Dot, Clock} = update_clock(ActorOrDot, Clock0),
    apply_ops(Ops, Dot, {Clock, Values, Deferred}, Ctx).

%% @private update the clock, and get a dot for the operations. This
%% means that field removals increment the clock too.
-spec update_clock(riak_dt:actor() | riak_dt:dot(),
                   riak_dt_vclock:vclock()) ->
                          {riak_dt:dot(), riak_dt_vclock:vclock()}.
update_clock(Dot, Clock) when is_tuple(Dot) ->
    NewClock = riak_dt_vclock:merge([[Dot], Clock]),
    {Dot, NewClock};
update_clock(Actor, Clock) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
    {Dot, NewClock}.

%% @private
-spec apply_ops([map_field_update() | map_field_op()], riak_dt:dot(),
                {riak_dt_vclock:vclock(), entries() , deferred()}, context()) ->
                       {ok, map()} | precondition_error().
apply_ops([], _Dot, Map, _Ctx) ->
    {ok, Map};
apply_ops([{update, {_Name, Type}=Field, Op} | Rest], Dot, {Clock, Values, Deferred}, Ctx) ->
    {CRDT, TrimmedValues} = ordsets:fold(fun({{F, _D}, Value}=E, {Acc, ValuesAcc}) when F == Field ->
                                                 %% remove the tagged
                                                 %% value, as it will
                                                 %% be superseded by
                                                 %% the new update
                                                 {Type:merge(Acc, Value),
                                                  ordsets:del_element(E, ValuesAcc)};
                                            (_E, Acc) ->
                                                 Acc
                                         end,
                                         {Type:new(), Values},
                                         Values),
    CRDT1 = Type:parent_clock(Clock, CRDT),
    case Type:update(Op, Dot, CRDT1, Ctx) of
        {ok, Updated} ->
            NewValues = ordsets:add_element({{Field, Dot}, Updated}, TrimmedValues),
            apply_ops(Rest, Dot, {Clock, NewValues, Deferred}, Ctx);
        Error ->
            Error
    end;
apply_ops([{remove, Field} | Rest], Dot, Map, Ctx) ->
    case remove_field(Field, Map, Ctx)  of
        {ok, NewMap} ->
            apply_ops(Rest, Dot, NewMap, Ctx);
        E ->
            E
    end;
apply_ops([{add, {_Name, Mod}=Field} | Rest], Dot, {Clock, Values, Deferred}, Ctx) ->
    %% @TODO ¿Should an add read and replace, or stand alone? If it
    %% stands alone, a concurrent remove results in an empty field,
    %% which is nice. After an update though, we're back to a mixed,
    %% unknown result. And of course we have a duplicate entry. Adds
    %% are weird.
    ToAdd = {{Field, Dot}, Mod:new()},
    NewValues = ordsets:add_element(ToAdd, Values),
    apply_ops(Rest, Dot, {Clock, NewValues, Deferred}, Ctx).

%% @private when context is undefined, we simply remove all instances
%% of Field, regardless of their dot. If the field is not present then
%% we warn the user with a precondition error. However, in the case
%% that a context is provided we can be more fine grained, and only
%% remove those field entries whose dots are seen by the context. This
%% preserves the "observed" part of "observed-remove". There is no
%% precondition error if we're asked to remove smoething that isn't
%% present, either we defer it, or it has been done already, depending
%% on if the Map clock descends the context clock or not.
%%
%% @see defer_remove/4 for handling of removes of fields that are
%% _not_ present
-spec remove_field(field(), map(), context()) ->
                          {ok, map()} | precondition_error().
remove_field(Field, {Clock, Values, Deferred}, undefined) ->
    {Removed, NewValues} = ordsets:fold(fun({{F, _Dot}, _Val}, {_B, AccIn}) when F == Field ->
                                                {true, AccIn};
                                           (Elem, {B, AccIn}) ->
                                                {B, ordsets:add_element(Elem, AccIn)}
                                        end,
                                        {false, ordsets:new()},
                                        Values),
    case Removed of
        false ->
            {error, {precondition, {not_present, Field}}};
        _ ->
            {ok, {Clock, NewValues, Deferred}}
    end;
%% Context removes
remove_field(Field, {Clock, Values, Deferred0}, Ctx) ->
    Deferred = defer_remove(Clock, Ctx, Field, Deferred0),
    NewValues = ordsets:fold(fun({{F, Dot}, _Val}=E, AccIn) when F == Field ->
                                     keep_or_drop(Ctx, Dot, AccIn, E);
                                (Elem, AccIn) ->
                                     ordsets:add_element(Elem, AccIn)
                             end,
                             ordsets:new(),
                             Values),
    {ok, {Clock, NewValues, Deferred}}.

%% @private If we're asked to remove something we don't have (or have,
%% but maybe not having seen all 'adds' for it), is it because we've
%% not seen the add|update that we've been asked to remove, or is it
%% because we already removed it? In the former case, we can "defer"
%% this operation by storing it, with its context, for later
%% execution. If the clock for the Map descends the operation clock,
%% then we don't need to defer the op, its already been done. It is
%% _very_ important to note, that only _actorless_ operations can be
%% saved. That is operations that DO NOT increment the clock. In Map
%% this means field removals. Contexts for update operations do not
%% result in deferred operations on the parent Map.
-spec defer_remove(riak_dt_vclock:vclock(), riak_dt_vclock:vclock(), field(), deferred()) ->
                          deferred().
defer_remove(Clock, Ctx, Field, Deferred) ->
    case riak_dt_vclock:descends(Clock, Ctx) of
        %% no need to save this remove, we're done
        true -> Deferred;
        false -> orddict:update(Ctx,
                                fun(Fields) ->
                                        ordsets:add_element(Field, Fields) end,
                                ordsets:add_element(Field, ordsets:new()),
                                Deferred)
    end.

%% @doc merge two `map()'s.  and then a pairwise merge on all values
%% in the value list.  This is the LUB function.
-spec merge(map(), map()) -> map().
merge({LHSClock, LHSEntries, LHSDeferred}, {RHSClock, RHSEntries, RHSDeferred}) ->
    Clock = riak_dt_vclock:merge([LHSClock, RHSClock]),
    {Entries0, RHSUnique} = merge_left(LHSEntries, RHSEntries, RHSClock),
    Entries1 = merge_right(LHSClock, RHSUnique),
    Entries2 = ordsets:union(Entries0, Entries1),
    Deferred = merge_deferred(LHSDeferred, RHSDeferred),
    apply_deferred(Clock, Entries2, Deferred).

%% @private
-spec merge_deferred(deferred(), deferred()) -> deferred().
merge_deferred(LHS, RHS) ->
    orddict:merge(fun(_K, LH, RH) ->
                          ordsets:union(LH, RH) end,
                  LHS, RHS).

%% @private Merge the common entries, returns the merged common and
%% those unique to the right hand side.
-spec merge_left(entries(), entries(), riak_dt_vclock:vclock()) ->
                        {entries(), entries()}.
merge_left(LHSEntries, RHSEntries, RHSClock) ->
    lists:foldl(fun({{_F, Dot}=Key, _CRDT}=E, {Acc, RHS}) ->
                        case lists:keytake(Key, 1, RHS) of
                            {value, E, RHS1} ->
                                %% same in bolth
                                {ordsets:add_element(E, Acc), RHS1};
                            false ->
                                %% RHS does not have this field/dot, should be dropped, or kept?
                                Acc2 = keep_or_drop(RHSClock, Dot, Acc, E),
                                {Acc2, RHS}
                        end
                end,
                {ordsets:new(), RHSEntries},
                LHSEntries).

%% @private merge those elements that were unique to the right hand
%% side. Returns the subset of entries that should be kept.
-spec merge_right(riak_dt_vclock:vclock(), entries()) -> entries().
merge_right(LHSClock, RHSUnique) ->
    lists:foldl(fun({{_F, Dot}, _CRDT}=E, Acc) ->
                        keep_or_drop(LHSClock, Dot, Acc, E)
                end,
                ordsets:new(),
                RHSUnique).

%% @private decide (using `Clock') if the `Field' with `Tag' gets into
%% `Entries' or not.
-spec keep_or_drop(riak_dt_vclock:vclock(), riak_dt:dot(), entries(), entry()) ->
                          entries().
keep_or_drop(Clock, Tag, Entries, Field) ->
    case riak_dt_vclock:descends(Clock, [Tag]) of
        true ->
            Entries;
        false ->
            ordsets:add_element(Field, Entries)
    end.


%% @private apply those deferred field removals, if they're
%% preconditions have been met, that is.
-spec apply_deferred(riak_dt_vclock:vclock(), entries(), deferred()) ->
                            {riak_dt_vclock:vclock(), entries(), deferred()}.
apply_deferred(Clock, Entries, Deferred) ->
    lists:foldl(fun({Ctx, Fields}, Map) ->
                        remove_all(Fields, Map, Ctx)
                end,
                {Clock, Entries, []},
                Deferred).

%% @private
-spec remove_all([field()], map(), context()) ->
                        map().
remove_all(Fields, Map, Ctx) ->
    lists:foldl(fun(Field, MapAcc) ->
                        {ok, MapAcc2}= remove_field(Field, MapAcc, Ctx),
                        MapAcc2
                end,
                Map,
                Fields).

%% @doc compare two `map()'s for equality of structure Both schemas
%% and value list must be equal. Performs a pariwise equals for all
%% values in the value lists
-spec equal(map(), map()) -> boolean().
equal({Clock1, Values1, Deferred1}, {Clock2, Values2, Deferred2}) ->
    riak_dt_vclock:equal(Clock1, Clock2) andalso
        Deferred1 == Deferred2 andalso
        pairwise_equals(ordsets:to_list(Values1), ordsets:to_list(Values2)).

%% @Private Note, only called when we know that 2 sets of fields are
%% equal. Both dicts therefore have the same set of keys.
-spec pairwise_equals(entries(), entries()) -> boolean().
pairwise_equals([], []) ->
    true;
pairwise_equals([{{{Name, Type}, Dot}, CRDT1}| Rest1], [{{{Name, Type}, Dot}, CRDT2}|Rest2]) ->
    case Type:equal(CRDT1, CRDT2) of
        true ->
            pairwise_equals(Rest1, Rest2);
        false ->
            false
    end;
pairwise_equals(_, _) ->
    false.

%% @doc an opaque context that can be passed to `update/4' to ensure
%% that only seen fields are removed. If a field removal operation has
%% a context that the Map has not seen, it will be deferred until
%% causally relevant.
-spec precondition_context(map()) -> riak_dt:context().
precondition_context({Clock, _Field, _Deferred}) ->
    Clock.

%% @doc stats on internal state of Map.
%% A proplist of `{StatName :: atom(), Value :: integer()}'. Stats exposed are:
%% `actor_count': The number of actors in the clock for the Map.
%% `field_count': The total number of fields in the Map (including divergent field entries).
%% `duplication': The number of duplicate entries in the Map across all fields.
%%                basically `field_count' - ( unique fields)
%% `deferred_length': How many operations on the deferred list, a reasonable expression
%%                   of lag/staleness.
-spec stats(map()) -> [{atom(), integer()}].
stats(Map) ->
    [ {S, stat(S, Map)} || S <- [actor_count, field_count, duplication, deferred_length]].

-spec stat(atom(), map()) -> number() | undefined.
stat(actor_count, {Clock, _, _}) ->
    length(Clock);
stat(field_count, {_, Fields, _}) ->
    length(Fields);
stat(duplication, {_, Fields, _}) ->
    %% Number of duplicated fields
    DeDuped = ordsets:fold(fun({{Field, _}, _}, Acc) ->
                          ordsets:add_element(Field, Acc) end,
                  ordsets:new(),
                  Fields),
    length(Fields) - length(DeDuped);
stat(deferred_length, {_, _, Deferred}) ->
    length(Deferred);
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

%% This fails on previous version of riak_dt_map
assoc_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    {ok, B} = update({update, [{update, Field, {add, 0}}]}, b, new()),
    {ok, B2} = update({update, [{update, Field, {remove, 0}}]}, b, B),
    C = A,
    {ok, C3} = update({update, [{remove, Field}]}, c, C),
    ?assertEqual(merge(A, merge(B2, C3)), merge(merge(A, B2), C3)),
    ?assertEqual(value(merge(merge(A, C3), B2)), value(merge(merge(A, B2), C3))),
    ?assertEqual(merge(merge(A, C3), B2),  merge(merge(A, B2), C3)).

clock_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    B = A,
    {ok, B2} = update({update, [{update, Field, {add, 1}}]}, b, B),
    {ok, A2} = update({update, [{update, Field, {remove, 0}}]}, a, A),
    {ok, A3} = update({update, [{remove, Field}]}, a, A2),
    {ok, A4} = update({update, [{update, Field, {add, 2}}]}, a, A3),
    AB = merge(A4, B2),
    ?assertEqual([{Field, [1, 2]}], value(AB)).

remfield_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    B = A,
    {ok, A2} = update({update, [{update, Field, {remove, 0}}]}, a, A),
    {ok, A3} = update({update, [{remove, Field}]}, a, A2),
    {ok, A4} = update({update, [{update, Field, {add, 2}}]}, a, A3),
    AB = merge(A4, B),
    ?assertEqual([{Field, [2]}], value(AB)).

%% Bug found by EQC, not dropping dots in merge when an element is
%% present in both Maos leads to removed items remaining after merge.
present_but_removed_test() ->
    %% Add Z to A
    {ok, A} = update({update, [{add, {'Z', riak_dt_lwwreg}}]}, a, new()),
    %% Replicate it to C so A has 'Z'->{a, 1}
    C = A,
    %% Remove Z from A
    {ok, A2} = update({update, [{remove, {'Z', riak_dt_lwwreg}}]}, a, A),
    %% Add Z to B, a new replica
    {ok, B} = update({update, [{add, {'Z', riak_dt_lwwreg}}]}, b, new()),
    %%  Replicate B to A, so now A has a Z, the one with a Dot of
    %%  {b,1} and clock of [{a, 1}, {b, 1}]
    A3 = merge(B, A2),
    %% Remove the 'Z' from B replica
    {ok, B2} = update({update, [{remove, {'Z', riak_dt_lwwreg}}]}, b, B),
    %% Both C and A have a 'Z', but when they merge, there should be
    %% no 'Z' as C's has been removed by A and A's has been removed by
    %% C.
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         %% the order matters, the two replicas that
                         %% have 'Z' need to merge first to provoke
                         %% the bug. You end up with 'Z' with two
                         %% dots, when really it should be removed.
                         A3,
                         [C, B2]),
    ?assertEqual([], value(Merged)).


%% A bug EQC found where dropping the dots in merge was not enough if
%% you then store the value with an empty clock (derp).
no_dots_left_test() ->
    {ok, A} =  update({update, [{add, {'Z', riak_dt_lwwreg}}]}, a, new()),
    {ok, B} =  update({update, [{add, {'Z', riak_dt_lwwreg}}]}, b, new()),
    C = A, %% replicate A to empty C
    {ok, A2} = update({update, [{remove, {'Z', riak_dt_lwwreg}}]}, a, A),
    %% replicate B to A, now A has B's 'Z'
    A3 = merge(A2, B),
    %% Remove B's 'Z'
    {ok, B2} = update({update, [{remove, {'Z', riak_dt_lwwreg}}]}, b, B),
    %% Replicate C to B, now B has A's old 'Z'
    B3 = merge(B2, C),
    %% Merge everytyhing, without the fix You end up with 'Z' present,
    %% with no dots
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         A3,
                         [B3, C]),
    ?assertEqual([], value(Merged)).


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
    %% How big is a Map? Maybe number of fields and depth matter? But
    %% then the number of fields in sub maps too?
    byte_size(term_to_binary(Map)) div 10.

generate() ->
        ?LET({Ops, Actors}, {non_empty(list(gen_op())), non_empty(list(bitstring(16*8)))},
         lists:foldl(fun(Op, Map) ->
                             Actor = case length(Actors) of
                                         1 -> hd(Actors);
                                         _ -> lists:nth(crypto:rand_uniform(1, length(Actors)), Actors)
                                     end,
                             case update(Op, Actor, Map) of
                                 {ok, M} -> M;
                                 _ -> Map
                             end
                     end,
                     new(),
                     Ops)).

gen_op() ->
    ?LET(Ops, non_empty(list(gen_update())), {update, Ops}).

gen_update() ->
    ?LET(Field, gen_field(),
         oneof([{add, Field}, {remove, Field},
                {update, Field, gen_field_op(Field)}])).

gen_field() ->
    {non_empty(binary()), oneof([riak_dt_pncounter,
                                 riak_dt_orswot,
                                 riak_dt_lwwreg,
                                 riak_dt_map,
                                 riak_dt_od_flag])}.

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

%% found by eqc. Using tag as key in merge left causes a bug like
%% this: A adds field X, Y at {a, 1} A replicates to b be removes
%% field X A merges with B Now keytake had a function clause exception
%% because lists:keytake({a, 1}, 3) is neither the same element nor
%% notfound
dot_key_test() ->
    {ok, A} = update({update, [{add, {'X', riak_dt_orswot}}, {add, {'X', riak_dt_od_flag}}]}, a, new()),
    B = A,
    {ok, A2} = update({update, [{remove, {'X', riak_dt_od_flag}}]}, a, A),
    ?assertEqual([{{'X', riak_dt_orswot}, []}], value(merge(B, A2))).

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
    ?assertEqual([{actor_count, 0}, {field_count, 0}, {duplication, 0}, {deferred_length, 0}], stats(Map)),
    ?assertEqual(3, stat(actor_count, Map4)),
    ?assertEqual(6, stat(field_count, Map4)),
    ?assertEqual(undefined, stat(waste_pct, Map4)),
    ?assertEqual(1, stat(duplication, Map4)),
    {ok, Map5} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"baz">>, 2}}]}, a3, Map4),
    ?assertEqual(5, stat(field_count, Map5)),
    ?assertEqual(0, stat(duplication, Map5)).

-endif.
