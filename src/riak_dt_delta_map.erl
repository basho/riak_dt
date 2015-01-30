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
%% is the name of a crdt module that may be embedded. CRDTs stored
%% inside the Map will have their `update/3/4' function called, but the
%% second argument will be a `riak_dt:dot()', so that they share the
%% causal context of the map, even when fields are removed, and
%% subsequently re-added.
%%
%% The contents of the Map are modeled as a dictionary of
%% `field_name()' to `field_value()' mappings. Where `field_ name()'
%% is a two tuple of an opaque `binary()' name, and one of the
%% embeddable crdt types (currently `riak_dt_orswot',
%% `riak_dt_emcntr', `riak_dt_lwwreg', `riak_dt_od_flag', and
%% `riak_dt_map'). The reason for this limitation is that embedded
%% types must support embedding: that is a shared, `dot'-based, causal
%% context, and a reset-remove semantic (more on these below.)  The
%% `field_value()' is a two-tuple of `entries()' and a
%% `tombstone()'. The presence of a `tombstone()' in a "tombstoneless"
%% Map is confusing. The `tombstone()' is only stored for fields that
%% are currently in the map, removing a field also removes its
%% tombstone.
%%
%% To use the Map create a `new()' Map. When you call `update/3' or
%% `update/4' you pass a list of operations and an optional causal
%% context. @See `update/3' or `update/4' for more details. The list
%% of operations is applied atomically in full, and new state
%% returned, or not at all, and an error is returned.
%%
%% <h2>Semantics</h2>
%%
%% The semantics of this Map are Observed-Remove-Reset-Remove. What
%% this means is practice is, if a field is removed, and concurrently
%% that same field is updated, the field is _in_ the Map (only
%% observed updates are removed) but those removes propagate, so only
%% the concurrent update survives. A concrete example helps: If a Map
%% contains a field that is a set, and the set has 5 elements in it,
%% and concurrently the replica at A removes the field that contains
%% the set, while the replica at B adds an item to the set, on merge
%% there is a field for the set, but it contains only the one item B
%% added. The removal of the field is semantically equivalent to
%% removing all elements in the set, and removing the field. The same
%% goes for an embedded Map. If concurrently a Map field is removed,
%% while a new sub-field is updated, only the updated field(s) survive
%% the reset-remove.
%%
%% There is an anomaly for embedded counters that does not fully
%% support reset remove. Embedded counters (@see riak_dt_emcntr) are
%% different to a normal `pn-counter'. Embedded counters map `dot's to
%% {P, N} pairs. When a counter is incremented a new dot is created,
%% that replaces the old dot with the new value. `pn-counter' usually
%% merges by taking the `max' of any `P' or `N' entry for an
%% actor. This does not work in an embedded context. When a counter
%% field is removed, and then _re_-added, the new `P' and `N' entries
%% may be lower than the old, and merging loses the remove
%% information. However, if a `dot' is stored with the value, and the
%% max of the `dot' is used in merge, new updates win over removed
%% updates. So far so good. Here is the problem. If Replica B removes
%% a counter field, and does not re-add it, and replica A concurrently
%% updates it's entry for that field, then the reset-remove does not
%% occur. All new dots are not `observed' by Replica B, so not
%% removed. The new `dots' contain the updates from the previous
%% `dots', and the old `dot' is discarded. To achieve reset-remove all
%% increments would need a dot, and need to be retained, which would
%% be very costly in terms of space. One way to accept this anomaly is
%% to think of a Map like a file system: removing a directory and
%% concurrently adding a file means that the directory is present and
%% only the file remains in it. Updating a counter and concurrently
%% removing it, means the counter remains, with the updated value,
%% much like appending to a file in the file system analogy: you don't
%% expect only the diff to survive, but the whole updated file.
%%
%% <h2>Merging/Size</h2>
%%
%% When any pair of Maps are merged, the embedded CRDTs are _not_
%% merged, instead each concurrent `dot'->`field()' entry is
%% kept. This leads to a greater size for Maps that are highly
%% divergent. Updating a field in the map, however, leads to all
%% entries for that field being merged to a single CRDT that is stored
%% against the new `dot'. As mentioned above, there is also a
%% `tombstone' entry per present field. This is bottom CRDT for the
%% field type with a clock that contains all seen and removed
%% `dots'. There tombstones are merged at merge time, so only one is
%% present per field. Clearly the repetition of actor information (the
%% clock, each embedded CRDT, the field `dots', the tombstones) is a
%% serious issue with regard to size/bloat of this data type. We use
%% erlang's `to_binary/2' function, which compresses the data, to get
%% around this at present.
%%
%% <h2>Context and Deferred operations</h2>
%%
%% For CRDTs that use version vectors and dots (this `Map' and all
%% CRDTs that may be embedded in it), the size of the CRDT is
%% influenced by the number of actors updating it. In some systems
%% (like Riak!) we attempt to minimize the number of actors by only
%% having the database update CRDTs. This leads to a kind of "action
%% at a distance", where a client sends operations to the database,
%% and an actor in the database system performs the operations. The
%% purpose is to ship minimal state between database and client, and
%% to limit the number of actors in the system. There is a problem
%% with action at a distance and the OR semantic. The client _must_ be
%% able to tell the database what has been observed when it sends a
%% remove operation. There is a further problem. A replica that
%% handles an operation may not have all the state the client
%% observed. We solve these two problems by asking the client to
%% provide a causal context for operations (@see `update/4'.) Context
%% operations solve the OR problem, but they don't solve the problem
%% of lagging replicas handling operations.
%%
%% <h3>Lagging replicas, deferred operations</h3>
%%
%% In a system like Riak, a replica that is not up-to-date (including,
%% never seen any state for a CRDT) maybe asked to perform an
%% operation. If no context is given, and the operation is a field
%% remove, or a "remove" like operation on an embedded CRDT, the
%% operation may fail with a precondition error (for example, remove a
%% field that is not present) or succeed and remove more state than
%% intended (a field remove with no context may remove updates unseen
%% by the client.) When a context is provided, and the Field to be
%% removed is absent, the Map state stores the context, and Field
%% name, in a list of deferred operations. When, eventually, through
%% propagation and merging, the Map's clock descends the context for
%% the operation, the operation is executed. It is important to note
%% that _only_ actorless (field remove) operations can occur this way.
%%
%% <h4>Embedded CRDTs Deferred Operations</h4>
%%
%% There is a bug with embedded types and deferred operations. Imagine
%% a client has seen a Map with a Set field, and the set contains {a,
%% b, c}. The client sends an operation to remove {a} from the set. A
%% replica that is new takes the operation. It will create a new Map,
%% a Field for the Set, and store the `remove` operation as part of
%% the Set's state. A client reads this new state, and sends a field
%% remove operation, that is executed by same replica. Now the
%% deferred operation is lost, since the field is removed. We're
%% working on ways to fix this. One idea is to not remove a field with
%% "undelivered" operations, but instead to "hide" it.
%%
%% @see riak_dt_orswot for more on the OR semantic
%% @see riak_dt_emcntr for the embedded counter.
%% @end

-module(riak_dt_delta_map).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, update/3, delta_update/3, delta_update/4, update/4]).
-export([merge/2, equal/2, to_binary/1, from_binary/1]).
-export([to_binary/2, from_binary/2]).
-export([precondition_context/1, stats/1, stat/2]).
-export([parent_clock/2, get_deferred/1]).

%, get_deferred/2

%% EQC API
-ifdef(EQC).
-export([gen_op/0, gen_op/1, gen_field/0, gen_field/1,  generate/0, size/1]).
-endif.

-export_type([map/0, binary_map/0, map_op/0]).

-type binary_map() :: binary(). %% A binary that from_binary/1 will accept
-type map() :: {riak_dt_vclock:vclock(), entries(), deferred()}.
-type entries() :: [field()].
-type field() :: {field_name(), {field_meta(), field_value()}}.
-type field_name() :: {Name :: binary(), CRDTModule :: crdt_mod()}.
-type field_meta() :: {riak_dt_vclock:vclock(), seen(), tombstone()}.
-type field_value() :: crdt().

-type seen() :: dots().
-type deferred() :: [{context(), [field()]}].
-type tombstone() :: riak_dt_vclock:vclock().

-type dots() :: [dot()].
-type dot() :: riak_dt:dot().

%% limited to only those mods that support both a shared causal
%% context, and by extension, the reset-remove semantic.
-type crdt_mod() :: riak_dt_emcntr | riak_dt_lwwreg |
riak_dt_od_flag |
riak_dt_map | riak_dt_delta_map | riak_dt_orswot.

-type crdt()  ::  riak_dt_emcntr:emcntr() | riak_dt_od_flag:od_flag() |
riak_dt_lwwreg:lwwreg() | riak_dt_orswot:orswot() |
riak_dt_map:map() | riak_dt_delta_map:map().

-type map_op() :: {update, [map_field_update() | map_field_op()]}.

-type map_field_op() ::  {remove, field()}.
-type map_field_update() :: {update, field(), crdt_op()}.

-type crdt_op() :: riak_dt_emcntr:emcntr_op() |
riak_dt_lwwreg:lwwreg_op() | riak_dt_orswot:orswot_op() |
riak_dt_od_flag:od_flag_op() | riak_dt_map:map_op() | riak_dt_delta_map:map_op().

-type context() :: riak_dt_vclock:vclock() | undefined.

-type values() :: [value()].
-type value() :: {field(), riak_dt_delta_map:values() | integer() |
                  [term()] | boolean() | term()}.

-type precondition_error() :: {error, {precondition, {not_present, field()}}}.

-define(FRESH_CLOCK, riak_dt_vclock:fresh()).
-define(DICT, dict).

%% @doc Create a new, empty Map.
-spec new() -> map().
new() ->
    {riak_dt_vclock:fresh(), ?DICT:new(), ?DICT:new()}.

%% @doc get the current set of values for this Map
-spec value(map()) -> values().
value({Clock, Values, _Deferred}) ->
    lists:sort(?DICT:fold(
                  fun({Name, Type}, {{Dots, _, Tombstone}, CRDT0}, Acc) ->
                          %%TODO: Add this change to the normal map
                          CRDT = Type:parent_clock(Clock, CRDT0),
                          case Tombstone of
                              [] -> [{{Name, Type}, Type:value(CRDT)} | Acc];
                              _ -> 
                                  case riak_dt_vclock:descends(Tombstone, Dots) of
                                      true -> 
                                          Acc;
                                      false -> 
                                          case Type of
                                              riak_dt_delta_map -> 
                                                  SubTree = value(CRDT),
                                                  case SubTree of
                                                      [] -> 
                                                          Acc;
                                                      _ -> [{{Name, Type}, Type:value(CRDT)} | Acc]
                                                  end;
                                              _ ->
                                                  [{{Name, Type}, Type:value(CRDT)} | Acc]
                                          end
                                  end
                          end
                  end,
                  [],
                  Values)).

%% @doc query map (not implemented yet)
%%
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
%%  `{remove, `field()'}' where field is `{name, type}', results in
%%  the crdt at `field' and the key and value being removed. A
%%  concurrent `update' will "win" over a remove so that the field is
%%  still present, and it's value will contain the concurrent update.
%%
%% Atomic, all of `Ops' are performed successfully, or none are.
-spec update(map_op(), riak_dt:actor() | riak_dt:dot(), map()) ->
    {ok, map()} | precondition_error().
update(Op, ActorOrDot, Map) ->
    update(Op, ActorOrDot, Map, undefined).

%% @doc the same as `update/3' except that the context ensures no
%% unseen field updates are removed, and removal of unseen updates is
%% deferred. The Context is passed down as the context for any nested
%% types. hence the common clock.
%%
%% @see parent_clock/2
-spec update(map_op(), riak_dt:actor() | riak_dt:dot(), map(), riak_dt:context()) ->
    {ok, map()}.
update({update, Ops}, ActorOrDot, {Clock0, Values, Deferred}, Ctx) ->
    {Dot, Clock} = update_clock(ActorOrDot, Clock0),
    apply_ops(Ops, Dot, {Clock, Values, Deferred}, Ctx).

%% @private
-spec apply_ops([map_field_update() | map_field_op()], riak_dt:dot(),
                {riak_dt_vclock:vclock(), entries() , deferred()}, context()) ->
    {ok, map()} | precondition_error().
apply_ops([], _Dot, Map, _Ctx) ->
    {ok, Map};
apply_ops([{update, {_Name, Type}=Field, Op} | Rest], Dot, {Clock, Values, Deferred}, Ctx) ->
    {_,CRDT} = get_entry(Field, Values, Clock),
    case Type:update(Op, Dot, CRDT, Ctx) of
        {ok, Updated0} ->
            Updated = Type:parent_clock(?FRESH_CLOCK, Updated0),
            NewValues = case ?DICT:find(Field,Values) of
                            {ok, {{_,_,Tombstone}, _}} ->
                                ?DICT:store(Field, {{[Dot], [], Tombstone}, Updated}, Values);
                            error ->
                                ?DICT:store(Field, {{[Dot], [],  riak_dt_vclock:fresh()}, Updated}, Values)
                        end,
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
    end.

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
    case ?DICT:find(Field, Values) of
        error ->
            {error, {precondition, {not_present, Field}}};
        {ok, _Removed} ->
            {ok, {Clock, ?DICT:erase(Field, Values), Deferred}}
    end;

remove_field(Field, {Clock, Values, Deferred0}, Ctx) ->
    Deferred = defer_remove(Clock, Ctx, Field, Deferred0),
    {DefCtx, UpdtValues} = propagate_remove(Field, Values, Clock, Ctx),
    NewValues = case ?DICT:find(Field, UpdtValues) of
                    %Element is removed but has deferred operations
                    {ok, empty} when DefCtx =/= no_deferred ->
                        ?DICT:update(Field, 
                                     fun({{Dots, _S, Tombstone}, CRDT}) ->
                                             Tombstone = riak_dt_vclock:merge([DefCtx, Tombstone]),
                                             {{Dots, _S, Tombstone}, CRDT}
                                     end,UpdtValues);
                    {ok, empty} ->
                        ?DICT:erase(Field, UpdtValues);
                    {ok, CRDT} ->
                        ?DICT:store(Field, CRDT, UpdtValues);
                    error ->
                        UpdtValues
                end,
    {ok, {Clock, NewValues, Deferred}}.

%% @private drop dominated fields
ctx_rem_field(_Field, error, _Ctx_, _Clock) ->
    empty;

ctx_rem_field({_, Type}, {ok, {{Dots, _S, Tombstone}, CRDT}}, Ctx, MapClock) ->
    %% Drop dominated fields, and update the tombstone.
    %%
    %% If the context is removing a field at dot {a, 1} and the
    %% current field is {a, 2}, the tombstone ensures that all events
    %% from {a, 1} are removed from the crdt value. If the ctx remove
    %% is at {a, 3} and the current field is at {a, 2} then we need to
    %% remove only events upto {a, 2}. The glb clock enables that.
    %%
    TombstoneClock = riak_dt_vclock:glb(Ctx, MapClock), %% GLB is events seen by both clocks only
    TS = Type:parent_clock(TombstoneClock, Type:new()),
    SurvivingDots = riak_dt_vclock:subtract_dots(Dots, Ctx),
    case SurvivingDots of
        [] -> %% Ctx remove removed all dots for field
            empty;
        _ ->
            %% Update the tombstone with the GLB clock
            CRDT2 = Type:merge(TS, Type:parent_clock(MapClock, CRDT)),
            %% Always reset to empty clock so we don't duplicate storage
            {{SurvivingDots, _S, Tombstone}, Type:parent_clock(?FRESH_CLOCK, CRDT2)}
    end;
ctx_rem_field(Field, Values, Ctx, MapClock) ->
    ctx_rem_field(Field, ?DICT:find(Field, Values), Ctx, MapClock).

%% Value is a map:
%% Remove fields that don't have deferred operations;
%% Compute the removal tombstone for this field.
propagate_remove({_, riak_dt_delta_map}, {{Dots, _S, Tombstone}, {Clock, Value0, Deferred}}, MapClock, Ctx)->
    {SubMergedDef, SubEntries} =
    ?DICT:fold(fun(K, V, {UpdtClock, UpdtEntries}) ->
                       case propagate_remove(K, V, MapClock, Ctx) of
                          {_, empty} ->
                              {UpdtClock, UpdtEntries};
                          {TombstoneClock, Value} -> 
                              %%Some deferred operation in subtree
                              %%    - keep entry, update tombstones
                              {riak_dt_vclock:merge([TombstoneClock, UpdtClock]), 
                               ?DICT:store(K, Value, UpdtEntries)}
                      end
              end, {riak_dt_vclock:fresh(),?DICT:new()},Value0),
    %Clear map if all entries are empty
    case ?DICT:size(SubEntries) of
        0 ->
            {SubMergedDef, empty};
        _ -> {SubMergedDef, {{Dots, _S, riak_dt_vclock:merge([SubMergedDef | Tombstone])}, {Clock, SubEntries, Deferred}}}
    end;


%% Value is a leaf:
%% Merge deferred operations' context with Value clock (Tombstone) and send it upstream
%% Exclude non-covered dots -- intersection -- how does this relate to the second TODO?
%% Only handles half of the problem.
propagate_remove({_, Type}=Field, {{Clock, _S, TombstoneIn}, CRDT}, MapClock, Ctx) ->
    case Type:get_deferred(CRDT) of
        [] ->
            {[], ctx_rem_field(Field, {ok, {{Clock, _S, []}, CRDT}}, Ctx, MapClock)};
        _ ->
            Intersection = riak_dt_vclock:glb(Clock,Ctx),
            Tombstone = riak_dt_vclock:merge([Intersection, TombstoneIn | Type:get_deferred(CRDT)]),
            
            %% Clear CRDT
             TombstoneClock = riak_dt_vclock:glb(MapClock, Ctx),
             TS = Type:parent_clock(TombstoneClock, Type:new()),
             ClearedCRDT = Type:merge(TS, Type:parent_clock(TombstoneClock, CRDT)),
            {Tombstone, {{Clock, _S, Tombstone}, ClearedCRDT}}
    end;

propagate_remove(Field, Values, MapClock, Ctx) ->
    case ?DICT:find(Field, Values) of
        {ok,Value} -> 
            {UpdtClock,UpdtValue} = propagate_remove(Field, Value, MapClock, Ctx),
            case UpdtValue of
                empty ->
                    {UpdtClock, ?DICT:erase(Field, Values)};
                _ ->
                    {UpdtClock, ?DICT:store(Field, UpdtValue, Values)}
            end;
        error -> {MapClock, Values}
    end.

%% @private If we're asked to remove something we don't have (or have,
%% but maybe not all 'updates' for it), is it because we've not seen
%% the some update that we've been asked to remove, or is it because
%% we already removed it? In the former case, we can "defer" this
%% operation by storing it, with its context, for later execution. If
%% the clock for the Map descends the operation clock, then we don't
%% need to defer the op, its already been done. It is _very_ important
%% to note, that only _actorless_ operations can be saved. That is
%% operations that DO NOT need to increment the clock. In a Map this
%% means field removals only. Contexts for update operations do not
%% result in deferred operations on the parent Map. This simulates
%% causal delivery, in that an `update' must be seen before it can be
%% `removed'.
-spec defer_remove(riak_dt_vclock:vclock(), riak_dt_vclock:vclock(), field(), deferred()) ->
                          deferred().
defer_remove(Clock, Ctx, Field, Deferred) ->
    case riak_dt_vclock:descends(Clock, Ctx) of
        %% no need to save this remove, we're done
        true -> Deferred;
        false -> ?DICT:update(Ctx,
                                fun(Fields) ->
                                        ordsets:add_element(Field, Fields) end,
                                ordsets:add_element(Field, ordsets:new()),
                                Deferred)
    end.

-spec delta_update(map_op(), riak_dt:actor() | riak_dt:dot(), map()) ->
    {ok, map()} | precondition_error().
delta_update(Op, ActorOrDot, {Clock0, _, _}=Map) ->
    {Dot, _Clock} = update_clock(ActorOrDot, Clock0),
    delta_update(Op, Dot, Map, undefined).

-spec delta_update(map_op(), riak_dt:actor() | riak_dt:dot(), map(), riak_dt:context()) ->
    {ok, map()}.
delta_update({update, Ops}, Actor, {Clock0, _, _}=Map, Ctx) ->
    {Dot, _Clock} = update_clock(Actor, Clock0),
    delta_apply_ops(Ops, Dot, Map, new(), Ctx).

%% How to accept multiple operations for the same object?
%%  Always execute operation over original state, merge deltas.
%% For now, just support update operations.

%% @private
-spec delta_apply_ops([map_field_update() | map_field_op()], riak_dt:dot(),
                {riak_dt_vclock:vclock(), entries() , deferred()}, 
                {riak_dt_vclock:vclock(), entries() , deferred()}, context()) ->
    {ok, map()} | precondition_error().
delta_apply_ops([], _Dot, _Map, Delta, _Ctx) ->
    {ok, Delta};
delta_apply_ops([{update, {_Name, Type}=Field, Op} | Rest], Dot, {Clock0, Values, Deferred}, {DClock, DValues, DDef}, Ctx) ->
    {_, CRDT} = get_entry(Field, DValues, Clock0),
    case Type:delta_update(Op, Dot, CRDT, Ctx) of
        {ok, Updated0} ->
            Updated = Type:parent_clock(?FRESH_CLOCK, Updated0),
            NewDValues = case ?DICT:find(Field,DValues) of
                             {ok, {{Dots, Seen, _}, _}} ->
                                 ?DICT:store(Field, {
                                               {ordsets:add_element(Dot, Dots),
                                                ordsets:add_element(Dot, Seen),
                                                DDef
                                               }, Updated}, DValues );
                             error ->
                                ?DICT:store(Field, {{[Dot], [Dot], riak_dt_vclock:fresh()}, Updated}, DValues)
                        end,
            delta_apply_ops(Rest, Dot, {Clock0, Values, Deferred}, {DClock, NewDValues, DDef}, Ctx);
        Error ->
            Error
    end;

%% Store tombstone for any operation. 
%% - can we remove branches that don't have deferred operations immediatly?
delta_apply_ops([{remove, {_, Type}=Field} | Rest], _Dot, {Clock0, CRDT, Def}, {_DC, DCRDT, DDeferred0}, Ctx) ->
    DDeferred = defer_remove(Clock0, Ctx, Field, DDeferred0),
    UpdtCRDT = ?DICT:update(Field, fun(Value) -> 
                                           delta_apply_remove(Field, Value, Clock0, Ctx) end,
                            {{[], [], Ctx}, Type:new()},
                            CRDT),
    UpdtDCRDT = ?DICT:store(Field, ?DICT:fetch(Field, UpdtCRDT), DCRDT),
    delta_apply_ops(Rest, _Dot, {Clock0, CRDT, Def}, {_DC, UpdtDCRDT, DDeferred}, Ctx).

delta_apply_remove({_, Type}=Field, {{Active, _Seen, Tombstone}, CRDT}, Clock, Ctx) ->
    SurvivingDots = riak_dt_vclock:subtract_dots(Active, Ctx),
    NewCRDT = case Type of
                  riak_dt_delta_map -> 
                      {_C, Value, _D} = CRDT,
                      NewValue = ?DICT:map(fun(FieldI, ValueI) -> 
                                                   delta_apply_remove(FieldI, ValueI, Clock, Ctx)
                                           end, Value),
                      {_C, NewValue, _D};
                  _ ->
                      %% Trick to clear the value.
                      %% Set the clock in the empty object.
                      %% This only works because the parent_clock is doing a merge
                      %% The solution should use a clear operation.
                      Type:parent_clock(Clock, Type:new())
              end,
    DeferredOps = Type:get_deferred(CRDT),
    ChildTombstone = get_tombstone(Field, NewCRDT),
    %%Putting everything in the delta tombstone... maybe too conservative
    NewTombstone = riak_dt_vclock:merge([Tombstone, ChildTombstone, Ctx | DeferredOps]),
    {{[], lists:subtract(Active, SurvivingDots), NewTombstone}, NewCRDT}.

merge({LHSClock, LHSEntries, LHSDeferred}, {RHSClock, RHSEntries, RHSDeferred}) ->
    Fields = lists:umerge(?DICT:fetch_keys(LHSEntries), ?DICT:fetch_keys(RHSEntries)),
    {AllDots, Entries} = lists:foldl(fun(Field, {DotsAcc, Acc}) ->
                        LHSEntry= get_entry(Field, LHSEntries, LHSClock),
                        RHSEntry = get_entry(Field, RHSEntries, RHSClock),
                        case update_entry(Field, LHSClock, LHSEntry, RHSClock, RHSEntry) of
                            empty -> {DotsAcc, Acc};
                            {{_, Seen, _},_} = NewEntry ->
                                NewDots = lists:umerge(Seen, DotsAcc),
                                {NewDots, ?DICT:store(Field, NewEntry, Acc)}
                        end
                end,
                {[], ?DICT:new()},
                Fields),
    {Clock, _Seen} = compress_seen(riak_dt_vclock:merge([LHSClock, RHSClock]), AllDots),
    Deferred = merge_deferred(LHSDeferred, RHSDeferred),
    CRDT0 = apply_deferred(Clock, Entries, Deferred),
    CRDT = clear_seen(Clock, CRDT0),
    clear_tombstones(CRDT).

update_entry({_, Type}, LHSClock, LHSEntry, RHSClock, RHSEntry) ->
    {{LHSDots, LHSS, LHSTomb}, LHSCRDT} = LHSEntry,
    {{RHSDots, RHSS, RHSTomb}, RHSCRDT} = RHSEntry,
        case keep_dots(LHSDots, RHSDots, LHSClock, RHSClock, LHSS, RHSS) of
        [] ->
            case riak_dt_vclock:merge([LHSTomb, RHSTomb]) of
                [] -> empty;
                Tomb ->
                    %% This is wrong, this clock assumes both sides have seen the same dots
                    %% it might work, but it does not respect the semantics 
                    %Clock0 = riak_dt_vclock:merge([LHSClock, RHSClock]),
                    %L = Type:parent_clock(Clock0, LHSCRDT),
                    %R = Type:parent_clock(Clock0,RHSCRDT),
                    %MergedCRDT = Type:merge(L, R),
                    MergedCRDT = Type:merge(LHSCRDT, RHSCRDT),
                    MergedTomb = riak_dt_vclock:merge([Tomb | Type:get_deferred(MergedCRDT)]),
                    {{[], [], MergedTomb}, Type:parent_clock(?FRESH_CLOCK, MergedCRDT)}
            end;
        Dots ->
            MergedCRDT = Type:merge(LHSCRDT, RHSCRDT),
            MergedTombstone = riak_dt_vclock:merge([LHSTomb, RHSTomb | Type:get_deferred(MergedCRDT)]),
            MergedSeen = lists:umerge(LHSS, RHSS),
            {{Dots, MergedSeen, MergedTombstone}, Type:parent_clock(?FRESH_CLOCK, MergedCRDT)}
    end.

keep_dots(LHSDots, RHSDots, LHSClock, RHSClock, LHSeen, RHSeen) ->
    CommonDots = sets:intersection(sets:from_list(LHSDots), sets:from_list(RHSDots)),
    LHSUnique = sets:to_list(sets:subtract(sets:from_list(LHSDots), CommonDots)),
    RHSUnique = sets:to_list(sets:subtract(sets:from_list(RHSDots), CommonDots)),
    LHRemoved = ordsets:subtract(LHSeen, LHSDots),
    RHRemoved = ordsets:subtract(RHSeen, RHSDots),
    LHSKeep = riak_dt_vclock:subtract_dots(LHSUnique, lists:umerge(RHRemoved, RHSClock)),
    RHSKeep = riak_dt_vclock:subtract_dots(RHSUnique, lists:umerge(LHRemoved, LHSClock)),
    riak_dt_vclock:merge([sets:to_list(CommonDots), LHSKeep, RHSKeep]).

%% @private
-spec merge_deferred(deferred(), deferred()) -> deferred().
merge_deferred(LHS, RHS) ->
    ?DICT:merge(fun(_K, LH, RH) ->
                          ordsets:union(LH, RH) end,
                  LHS, RHS).

%% @private apply those deferred field removals, if they're
%% preconditions have been met, that is.
-spec apply_deferred(riak_dt_vclock:vclock(), entries(), deferred()) ->
                            {riak_dt_vclock:vclock(), entries(), deferred()}.
apply_deferred(Clock, Entries, Deferred) ->
    ?DICT:fold(fun(Ctx, Fields, Map) ->
                       remove_all(Fields, Map, Ctx)
               end,
               {Clock, Entries, ?DICT:new()},
               Deferred).

compress_seen(Clock, Seen) ->
    lists:foldl(fun(Node, {ClockAcc, SeenAcc}) ->
                        Cnt = riak_dt_vclock:get_counter(Node, Clock),
                        Cnts = proplists:lookup_all(Node, Seen),
                        case compress(Cnt, Cnts) of
                            {Cnt, Cnts} ->
                                {ClockAcc, lists:umerge(Cnts, SeenAcc)};
                            {Cnt2, []} ->
                                {riak_dt_vclock:merge([[{Node, Cnt2}], ClockAcc]),
                                 SeenAcc};
                            {Cnt2, Cnts2} ->
                                {riak_dt_vclock:merge([[{Node, Cnt2}], ClockAcc]),
                                 lists:umerge(SeenAcc, Cnts2)}
                        end
                end,
                {Clock, []},
                proplists:get_keys(Seen)).

compress(Cnt, []) ->
    {Cnt, []};
compress(Cnt, [{_A, Cntr} | Rest]) when Cnt >= Cntr ->
    compress(Cnt, Rest);
compress(Cnt, [{_A, Cntr} | Rest]) when Cntr - Cnt == 1 ->
    compress(Cnt+1, Rest);
compress(Cnt, Cnts) ->
    {Cnt, Cnts}.

%% Remove any Seen dots that are covered by Clock.
clear_seen(SeenClock, {_Clock, CRDT0, Deferred}) ->
    CRDT = ?DICT:map(fun({_,Type}, {{Active, Seen0, Tombstone}, Value}) -> 
                Seen = riak_dt_vclock:subtract_dots(Seen0, SeenClock),
                case Type of
                    riak_dt_delta_map -> 
                        {{Active, Seen, Tombstone}, clear_seen(SeenClock, Value)};
                        _ -> {{Active, Seen, Tombstone}, Value}
                end
             end, CRDT0),
    {_Clock, CRDT, Deferred}.

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

%Eliminates the tombstone if it has been integrated in the object's clock.
clear_tombstones({Clock, Entries, Deferred}) ->
    FilteredEntries = 
    ?DICT:fold(fun(Field_i, Value_i, FilteredEntriesAcc) ->
                       clear_tombstones_handle(Field_i, Value_i, FilteredEntriesAcc, Clock)
               end, ?DICT:new(), Entries),
    {Clock, FilteredEntries, Deferred}.

clear_tombstones_handle({_, riak_dt_delta_map}=Field, {{Dots, _S, Tombstone}, {Clock, CRDT, Deferred}}, NewMap, MapClock) ->
    FilteredEntries = 
    ?DICT:fold(fun(Field_i, Value_i, FilteredEntriesAcc) ->
                       clear_tombstones_handle(Field_i, Value_i, FilteredEntriesAcc, MapClock)
               end, ?DICT:new(), CRDT),
    %%Distinguish between empty map and removed map. --- this was changed, maybe do the same to map.
    case ?DICT:size(FilteredEntries) == 0 of
        true when length(Tombstone) > 0 -> 
            case riak_dt_vclock:descends(MapClock, Tombstone) of
                true -> NewMap;
                false -> ?DICT:store(Field, {{Dots, _S, Tombstone}, {Clock, FilteredEntries, Deferred}}, NewMap)
            end;
        true -> ?DICT:store(Field, {{Dots, _S, []}, {Clock, FilteredEntries, Deferred}}, NewMap);
        false -> 
            case riak_dt_vclock:descends(MapClock, Tombstone) of
                true ->
                    ?DICT:store(Field, {{Dots, _S, []}, {Clock, FilteredEntries, Deferred}}, NewMap);
                false ->
                    ?DICT:store(Field, {{Dots, _S, Tombstone}, {Clock, FilteredEntries, Deferred}}, NewMap)
            end
    end;

clear_tombstones_handle(Field, {{Dots,  _S, Tombstone}, CRDT}=Value, NewMap, MapClock) ->
    TombstoneCovered = riak_dt_vclock:descends(MapClock, Tombstone),
    case TombstoneCovered of
        true ->
            %% Changed this line, because it was failing when Dots didn't have all previous dots
            %% Maybe the same correction could be applied to the normal map
            ReceivedUpdates = riak_dt_vclock:subtract_dots(Dots, Tombstone),
            case ReceivedUpdates of
                [] ->
                    NewMap;
                _ -> 
                    ?DICT:store(Field, {{Dots, _S, []}, CRDT}, NewMap)
            end;
        false ->
            ?DICT:store(Field, Value, NewMap)
    end.

%% @doc compare two `map()'s for equality of structure Both schemas
%% and value list must be equal. Performs a pariwise equals for all
%% values in the value lists
-spec equal(map(), map()) -> boolean().
equal({Clock1, Values1, Deferred1}, {Clock2, Values2, Deferred2}) ->
    riak_dt_vclock:equal(Clock1, Clock2) andalso
        Deferred1 == Deferred2 andalso
        pairwise_equals(lists:sort(?DICT:to_list(Values1)),
                        lists:sort(?DICT:to_list(Values2))).

-spec pairwise_equals(entries(), entries()) -> boolean().
pairwise_equals([], []) ->
    true;
pairwise_equals([{{Name, Type}, {{Dots1, S1, Tombstone1}, CRDT1}}|Rest1], [{{Name, Type}, {{Dots2, S2, Tombstone2}, CRDT2}}|Rest2]) ->
    case {riak_dt_vclock:equal(Dots1, Dots2), S1 == S2, Type:equal(CRDT1, CRDT2), riak_dt_vclock:equal(Tombstone1, Tombstone2)} of
        {true, true, true, true} ->
            pairwise_equals(Rest1, Rest2);
        _ ->
            false
    end;
pairwise_equals(_, _) ->
    false.

%% @doc sets the clock in the map to that `Clock'. Used by a
%% containing Map for sub-CRDTs
-spec parent_clock(riak_dt_vclock:vclock(), map()) -> map().
parent_clock(Clock, {MapClock, Values, Deferred}) ->
    {riak_dt_vclock:merge([MapClock, Clock]), Values, Deferred}.

get_deferred({_, _, Deferred}) ->
    lists:map(fun({Key, _}) -> Key end, ?DICT:to_list(Deferred)).

get_tombstone({_, riak_dt_delta_map}, {_, Value, _}) ->
    ?DICT:fold(fun(_, {{_, _, Tomb},_}, Acc) -> 
                       riak_dt_vclock:merge([Tomb, Acc])
               end, [], Value);

get_tombstone(_, _) -> [].

get_entry({_Name, Type}=Field, Fields, Clock) ->
    {{Dots, _S, Tombstone}, CRDT} = case ?DICT:find(Field, Fields) of
                                 {ok, Entry} ->
                                     Entry;
                                 error ->
                                     {{[], [], riak_dt_vclock:fresh()}, Type:new()}
                             end,
    {{Dots, _S, Tombstone}, Type:parent_clock(Clock, CRDT)}.

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
    [ {S, stat(S, Map)} || S <- [actor_count, field_count, deferred_length]].

-spec stat(atom(), map()) -> number() | undefined.
stat(actor_count, {Clock, _, _}) ->
    length(Clock);
stat(field_count, {_, Fields, _}) ->
    ?DICT:size(Fields);
stat(deferred_length, {_, _, Deferred}) ->
    ?DICT:size(Deferred);
stat(_,_) -> undefined.


-include("riak_dt_tags.hrl").
-define(TAG, ?DT_MAP_TAG).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

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
    to_binary(?V2_VERS, Map).

%% @private encode v1 maps as v2, and vice versa. The first argument
%% is the target binary type.
-spec to_binary(1 | 2, map()) -> binary_map().
to_binary(?V2_VERS, Map0) ->
    Map = to_v2(Map0),
    <<?TAG:8/integer, ?V2_VERS:8/integer, (riak_dt:to_binary(Map))/binary>>;
to_binary(?V1_VERS, Map0) ->
    Map = to_v1(Map0),
    <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(Map))/binary>>.

%% @private transpose a v1 map (orddicts) to a v2 (dicts)
-spec to_v2({riak_dt_vclock:vclock(), orddict:orddict() | dict(),
             orddict:orddict() | dict()}) ->
                   {riak_dt_vclock:vclock(), dict(), dict()}.
to_v2({Clock, Fields0, Deferred0}) when is_list(Fields0),
                                         is_list(Deferred0) ->
    Fields = ?DICT:from_list(Fields0),
    Deferred = ?DICT:from_list(Deferred0),
    {Clock, Fields, Deferred};
to_v2(S) ->
    S.

%% @private transpose a v2 map (dicts) to a v1 (orddicts)
-spec to_v1({riak_dt_vclock:vclock(), orddict:orddict() | dict(),
             orddict:orddict() | dict()}) ->
                   {riak_dt_vclock:vclock(), orddict:orddict(), orddict:orddict()}.
to_v1({_Clock, Fields0, Deferred0}=S) when is_list(Fields0),
                                            is_list(Deferred0) ->
    S;
to_v1({Clock, Fields0, Deferred0}) ->
    %% Must be dicts, there is no is_dict test though
    %% should we use error handling as logic here??
    Fields = riak_dt:dict_to_orddict(Fields0),
    Deferred = riak_dt:dict_to_orddict(Deferred0),
    {Clock, Fields, Deferred}.

%% @doc When the argument is a `binary_map()' produced by
%% `to_binary/1' will return the original `map()'.
%%
%% @see `to_binary/1'
-spec from_binary(binary_map()) -> map().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    Map = riak_dt:from_binary(B),
    %% upgrade v1 structure to v2
    to_v2(Map);
from_binary(<<?TAG:8/integer, ?V2_VERS:8/integer, B/binary>>) ->
    riak_dt:from_binary(B).

%% @doc When the 2nd argument is a `binary_map()' produced by
%% either `to_binary/1' or `to_binary/2' and the first is a valid
%% version (`1' or `2' at present) will return an `map()' in the
%% correct `TargetVersion'
-spec from_binary(TargetVersion :: 1 | 2, binary_map()) -> map().
from_binary(?V1_VERS, <<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    riak_dt:from_binary(B);
from_binary(?V1_VERS, <<?TAG:8/integer, ?V2_VERS:8/integer, B/binary>>) ->
    Map = riak_dt:from_binary(B),
    to_v1(Map);
from_binary(?V2_VERS, <<?TAG:8/integer, ?V2_VERS:8/integer, B/binary>>) ->
    riak_dt:from_binary(B);
from_binary(?V2_VERS, <<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    Map= riak_dt:from_binary(B),
    to_v2(Map).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).


updt_merge(Update, Actor, State, Ctx) ->
     {ok, D} = delta_update(Update, Actor, State, Ctx),
     {ok, merge(D, State)}.

-define(FIELD, {'X', riak_dt_delta_map}).
-define(FIELD_Y, {'Y', riak_dt_delta_map}).
-define(FIELD_X, {'X', riak_dt_delta_od_flag}).
-define(FIELD_A, {'X.A', riak_dt_delta_od_flag}).
-define(FIELD_B, {'X.B', riak_dt_delta_od_flag}).
-define(FIELD_S, {'X.S', riak_dt_delta_orswot}).

-define(ENABLE_FLAG_A, {update, [{update, ?FIELD, {update, [{update, ?FIELD_A, enable}]}}]}).
-define(ENABLE_FLAG_B, {update, [{update, ?FIELD, {update, [{update, ?FIELD_B, enable}]}}]}).
-define(DISABLE_FLAG_A, {update, [{update, ?FIELD, {update, [{update, ?FIELD_A, disable}]}}]}).
-define(DISABLE_FLAG_B, {update, [{update, ?FIELD, {update, [{update, ?FIELD_B, disable}]}}]}).

-define(REMOVE_FIELD_X,{update, [{remove, ?FIELD}]}).
-define(REMOVE_FIELD_XA, {update, [{update, ?FIELD, {update, [{remove, ?FIELD_A}]}}]}).
-define(REMOVE_FIELD_XB, {update, [{update, ?FIELD, {update, [{remove, ?FIELD_B}]}}]}).

-define(ENABLE_FLAG_XYA, {update, [{update, ?FIELD, {update, [{update, ?FIELD_Y, {update, [{update, ?FIELD_A, enable}]}}]}}]}).

-define(UPDT_MERGE(Update, Actor, State), updt_merge(Update, Actor, State, undefined)).
-define(UPDT_MERGE(Update, Actor, State, Ctx), updt_merge(Update, Actor, State, Ctx)).

keep_deferred_test() ->
    InitialState = new(),
    {ok, {CtxA1, _, _} = StateA1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, enable}]}, a, InitialState),
    {ok, {CtxB1, _, _} = StateB1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, disable}]}, b, InitialState, CtxA1),
    {ok, StateB2} = ?UPDT_MERGE({update, [{remove, ?FIELD_X}]}, b, StateB1, CtxB1),
    StateAB = merge(StateA1,StateB2),
    ?assertEqual([],value(StateAB)).

keep_multiple_deferred_test() ->
    InitialState = new(),
    {ok, {CtxA1, _, _} = StateA1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, enable}]}, a, InitialState),
    {ok, {CtxB1, _, _} = StateB1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, disable}]}, b, InitialState, CtxA1),
    {ok, StateB2} = ?UPDT_MERGE({update, [{remove, ?FIELD_X}]}, b, StateB1, CtxB1),
    {ok, StateB3} = ?UPDT_MERGE({update, [{update, ?FIELD_X, disable}]}, b, StateB2, [{c,1}]),
    StateAB = merge(StateA1, StateB3),
    ?assertEqual([{{'X',riak_dt_delta_od_flag},false}],value(StateAB)).

keep_deferred_with_concurrent_add_test() ->
    InitialState = new(),
    {ok, {CtxA1, _, _} = StateA1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, enable}]}, a, InitialState),
    {ok, StateA2} = ?UPDT_MERGE({update, [{update, ?FIELD_X, enable}]}, a, StateA1, CtxA1),
    {ok, {CtxB1, _, _} = StateB1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, disable}]}, b, InitialState, CtxA1),
    {ok, StateB2} = ?UPDT_MERGE({update, [{remove, ?FIELD_X}]}, b, StateB1, CtxB1),
    StateAB = merge(StateA2,StateB2),
    ?assertEqual([{{'X',riak_dt_delta_od_flag},true}],value(StateAB)).

keep_deferred_context_test() ->
    InitialState = new(),
    {ok, {CtxA1, _, _} = StateA1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, enable}]}, a, InitialState),
    {ok, StateB1} = ?UPDT_MERGE({update, [{update, ?FIELD_X, disable}]}, b, InitialState, CtxA1),
    {ok, StateB2} = ?UPDT_MERGE({update, [{remove, ?FIELD_X}]}, b, StateB1, CtxA1),
    ?assertEqual([{{'X',riak_dt_delta_od_flag},false}],value(StateB2)),
    StateAB = merge(StateA1,StateB2),
    ?assertEqual([{{'X',riak_dt_delta_od_flag},false}],value(StateAB)).

remove_subtree_test() ->
    InitialState = new(),
    {ok, {CtxA1, _, _} = StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {CtxB1, _, _} = StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),
    {ok, StateB2} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, StateB1, CtxB1),
    StateAB = merge(StateA1,StateB2),
    ?assertEqual([],value(StateAB)).

remove_entry_in_subtree_test() ->
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),
    {ok, StateB2} =  ?UPDT_MERGE(?REMOVE_FIELD_XA, b, StateB1, CtxB1),
    StateAB = merge(StateA1,StateB2),
    ?assertEqual([{{'X',riak_dt_delta_map},[]}],value(StateAB)).

remove_entry_in_subtree_2_test() ->
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),
    {ok, {CtxB2,_,_}=StateB2} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, StateB1, CtxB1),
    {ok, StateB3} = ?UPDT_MERGE(?ENABLE_FLAG_B, b, StateB2, CtxB2),
    StateAB = merge(StateA1,StateB3),
    ?assertEqual([{{'X',riak_dt_delta_map}, [{{'X.B',riak_dt_delta_od_flag},true}]}],value(StateAB)).

remove_entry_in_subtree_3_test() ->
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),
    {ok, {_,_,_}=StateB2} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, StateB1, CtxB1),
    {ok, StateA2} = ?UPDT_MERGE(?ENABLE_FLAG_B, a, StateA1, CtxA1),
    StateAB = merge(StateA2,StateB2),
    ?assertEqual([{{'X',riak_dt_delta_map}, [{{'X.B',riak_dt_delta_od_flag},true}]}],value(StateAB)).

two_deferred_entries_test() ->
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {CtxC1,_,_}=StateC1} = ?UPDT_MERGE(?ENABLE_FLAG_B, c, InitialState),
    {ok, {_CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),
    {ok, {CtxB2,_,_}=StateB2} = ?UPDT_MERGE(?DISABLE_FLAG_B, b, StateB1, CtxC1),
    {ok, {CtxB3,_,_}=StateB3} = ?UPDT_MERGE(?REMOVE_FIELD_XA, b, StateB2, CtxB2),
    {ok, {_CtxB4,_,_}=StateB4} = ?UPDT_MERGE(?REMOVE_FIELD_XB, b, StateB3, CtxB3),
    {_,Map,_}=StateAB = merge(StateA1,StateB4),

    %%Check that the element is there
    {ok, {{_,_,_},{_,X,_}}} = ?DICT:find(?FIELD,Map),
    ?assertEqual(true,?DICT:is_key(?FIELD_B,X)),
    StateABC = merge(StateAB,StateC1),
    ?assertEqual([{{'X',riak_dt_delta_map},[]}],value(StateABC)).

two_deferred_entries_2_test() ->
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {CtxC1,_,_}=StateC1} = ?UPDT_MERGE(?ENABLE_FLAG_B, c, InitialState),
    {ok, {CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),
    {ok, {_CtxB2,_,_}=StateB2} = ?UPDT_MERGE(?REMOVE_FIELD_XA, b, StateB1, CtxB1),
    {ok, {CtxB3,_,_}=StateB3} = ?UPDT_MERGE(?DISABLE_FLAG_B, b, StateB2, CtxC1),
    StateAB = merge(StateA1,StateB3),
    {ok, {_,_,_}=StateAB1} = ?UPDT_MERGE(?REMOVE_FIELD_XB, b, StateAB, CtxB3),
    StateABC = merge(StateAB1,StateC1),
    ?assertEqual([{{'X',riak_dt_delta_map},[]}],value(StateABC)).

clear_invisible_after_merge_test() ->
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {_,_,_}=StateA2} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, StateA1),
    {ok, {CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),
    {ok, {CtxB2,_,_}=StateB2} = ?UPDT_MERGE(?ENABLE_FLAG_B, b, StateB1, CtxB1),
    {ok, {_,_,_}=StateB3} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, StateB2, CtxB2),
    StateAB1 = merge(StateB3,StateA1),
    StateAB2 = merge(StateB3,StateA2),
    ?assertEqual([], value(StateAB1)),
    ?assertEqual([{{'X',riak_dt_delta_map},[{{'X.A',riak_dt_delta_od_flag},true}]}], value(StateAB2)).

clear_invisible_after_merge_2_test() ->
    InitialState = new(),
    {ok, {CtxA1,_,_}=_StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {_CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_B, b, InitialState,CtxA1),
    {ok, {CtxB2,Map,_}=StateB2} = ?UPDT_MERGE(?ENABLE_FLAG_XYA, b, StateB1),

    %%Check that the element is there
    {ok, {{_,_,_},{_,X,_}}} = ?DICT:find(?FIELD,Map),
    {ok, {{_,_,_},{_,Y,_}}} = ?DICT:find(?FIELD_Y, X),
    ?assertEqual(true,?DICT:is_key(?FIELD_A,Y)),
    {ok, {_,_,_}=StateB3} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, StateB2, CtxB2),
    ?assertEqual([], value(StateB3)).

clear_invisible_after_merge_set_test() ->
    AddElemToS = fun(Elem) ->
                         {update, [{update, ?FIELD, {update, [{update, ?FIELD_S, {add, Elem}}]}}]}
                 end,
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, InitialState),
    {ok, {CtxB1,_,_}=StateB1} = ?UPDT_MERGE(?DISABLE_FLAG_A, b, InitialState, CtxA1),       
    {ok, {CtxB2,_,_}=StateB2} = ?UPDT_MERGE(AddElemToS(0), b, StateB1, CtxB1),
    {ok, {CtxB3,_,_}=StateB3} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, StateB2, CtxB2),
    {ok, {_,_,_}=StateB4} = ?UPDT_MERGE(AddElemToS(1), b, StateB3, CtxB3),
    StateAB = merge(StateA1,StateB4),
    ?assertEqual([{{'X',riak_dt_delta_map}, [{{'X.S',riak_dt_delta_orswot},[1]}]}], value(StateAB)).

clear_invisible_after_merge_set_2_test() ->
    AddElemToS = fun(Elem) ->
                         {update, [{update, ?FIELD, {update, [{update, ?FIELD_S, {add, Elem}}]}}]}
                 end,
    RemElemFromS = fun(Elem) ->
                         {update, [{update, ?FIELD, {update, [{update, ?FIELD_S, {remove, Elem}}]}}]}
                 end,
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(AddElemToS(0), a, InitialState),
    {ok, {CtxB1,_,_}=StateB1} = ?UPDT_MERGE(RemElemFromS(0), b, InitialState, CtxA1),
    {ok, {CtxB2,_,_}=StateB2} = ?UPDT_MERGE(AddElemToS(1), b, StateB1, CtxB1),
    {ok, {CtxB3,_,_}=StateB3} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, StateB2, CtxB2),
    {ok, {_,_,_}=StateB4} = ?UPDT_MERGE(AddElemToS(2), b, StateB3, CtxB3),
    StateAB = merge(StateA1,StateB4),
    ?assertEqual([{{'X',riak_dt_delta_map}, [{{'X.S',riak_dt_delta_orswot},[2]}]}], value(StateAB)).

transaction_1_test() ->
    Updt1 = {update, [{update, ?FIELD, {update, [{update, ?FIELD_A, enable}, {update, ?FIELD_B, enable}]}}]},
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(Updt1, a, InitialState),
    ?assertEqual([{{'X',riak_dt_delta_map},[{{'X.A',riak_dt_delta_od_flag},true}, 
                                      {{'X.B',riak_dt_delta_od_flag},true}]}], value(StateA1)),
    {ok, StateB1} = ?UPDT_MERGE(?REMOVE_FIELD_X, b, InitialState, CtxA1),
    StateAB = merge(StateA1,StateB1),
    ?assertEqual([], value(StateAB)).

transaction_2_test() ->
    Updt1 = {update, [{update, ?FIELD, {update, [{update, ?FIELD_A, enable}, {update, ?FIELD_B, enable}]}}]},
    InitialState = new(),
    {ok, {CtxA1,_,_}=StateA1} = ?UPDT_MERGE(Updt1, a, InitialState),
    {ok, StateB1} = ?UPDT_MERGE(?REMOVE_FIELD_XA, b, InitialState, CtxA1),
    StateAB = merge(StateA1,StateB1),
    ?assertEqual([{{'X',riak_dt_delta_map},[{{'X.B',riak_dt_delta_od_flag},true}]}], value(StateAB)),
    {ok, StateA2} = ?UPDT_MERGE(?ENABLE_FLAG_A, a, StateA1),
    StateA2B = merge(StateA2,StateB1),
    ?assertEqual([{{'X',riak_dt_delta_map},[{{'X.A',riak_dt_delta_od_flag},true}, 
                                      {{'X.B',riak_dt_delta_od_flag},true}]}], value(StateA2B)).


%assoc_test() ->
%    Field = {'X', riak_dt_orswot},
%    {ok, A} = ?UPDT_MERGE({update, [{update, Field, {add, 0}}]}, a, new()),
%    {ok, B} = ?UPDT_MERGE({update, [{update, Field, {add, 0}}]}, b, new()),
%    {ok, B2} = ?UPDT_MERGE({update, [{update, Field, {remove, 0}}]}, b, B),
%    C = A,
%    {ok, C3} = ?UPDT_MERGE({update, [{remove, Field}]}, c, C),
%    ?assertEqual(merge(A, merge(B2, C3)), merge(merge(A, B2), C3)),
%    ?assertEqual(value(merge(merge(A, C3), B2)), value(merge(merge(A, B2), C3))),
%    ?assertEqual(merge(merge(A, C3), B2),  merge(merge(A, B2), C3)).

%clock_test() ->
%    Field = {'X', riak_dt_orswot},
%    {ok, A} = ?UPDT_MERGE({update, [{update, Field, {add, 0}}]}, a, new()),
%    B = A,
%    {ok, B2} = ?UPDT_MERGE({update, [{update, Field, {add, 1}}]}, b, B),
%    {ok, A2} = ?UPDT_MERGE({update, [{update, Field, {remove, 0}}]}, a, A),
%    {ok, A3} = ?UPDT_MERGE({update, [{remove, Field}]}, a, A2),
%    {ok, A4} = up?UPDT_MERGE({update, [{update, Field, {add, 2}}]}, a, A3),
%    AB = merge(A4, B2),
%    ?assertEqual([{Field, [1, 2]}], value(AB)).

%remfield_test() ->
%    Field = {'X', riak_dt_orswot},
%    {ok, A} = ?UPDT_MERGE({update, [{update, Field, {add, 0}}]}, a, new()),
%    B = A,
%    {ok, A2} = ?UPDT_MERGE({update, [{update, Field, {remove, 0}}]}, a, A),
%    {ok, A3} = ?UPDT_MERGE({update, [{remove, Field}]}, a, A2),
%    {ok, A4} = ?UPDT_MERGE({update, [{update, Field, {add, 2}}]}, a, A3),
%    AB = merge(A4, B),
%    ?assertEqual([{Field, [2]}], value(AB)).

%present_but_removed_test() ->
%    F = {'X', riak_dt_lwwreg},
%    {ok, A} = ?UPDT_MERGE({update, [{update, F, {assign, <<"A">>}}]}, a, new()),
%    C = A,
%    {ok, A2} = ?UPDT_MERGE({update, [{remove, F}]}, a, A),
%    {ok, B} = ?UPDT_MERGE({update, [{update, F, {assign, <<"B">>}}]}, b, new()),
%    A3 = merge(B, A2),
%   {ok, B2} = ?UPDT_MERGE({update, [{remove, F}]}, b, B),
%   Merged = lists:foldl(fun(Set, Acc) ->
%                                merge(Set, Acc) end,
%                        A3,
%                        [C, B2]),
%   ?assertEqual([], value(Merged)).


%no_dots_left_test() ->
%    F = {'Z', riak_dt_lwwreg},
%    {ok, A} =  ?UPDT_MERGE({update, [{update, F, {assign, <<"A">>}}]}, a, new()),
%    {ok, B} =  ?UPDT_MERGE({update, [{update, F, {assign, <<"B">>}}]}, b, new()),
%    C = A, %% replicate A to empty C
%    {ok, A2} = ?UPDT_MERGE({update, [{remove, F}]}, a, A),
%    A3 = merge(A2, B),
%    {ok, B2} = ?UPDT_MERGE({update, [{remove, F}]}, b, B),
%    B3 = merge(B2, C),
%    Merged = lists:foldl(fun(Set, Acc) ->
%                                merge(Set, Acc) end,
%                         A3,
%                         [B3, C]),
%    ?assertEqual([], value(Merged)).

%tombstone_remove_test() ->
%    F = {'X', riak_dt_orswot},
%    A=B=new(),
%    {ok, A1} = ?UPDT_MERGE({update, [{update, F, {add, 0}}]}, a, A),
%    B1 = merge(A1, B),
%    {ok, A2} = ?UPDT_MERGE({update, [{remove, F}]}, a, A1),
%    {ok, B2} = ?UPDT_MERGE({update, [{update, F, {add, 1}}]}, b, B1),
%    A3 = merge(A2, B2),
%    ?assertEqual([{F, [1]}], value(A3)),
%    {ok, B3} = ?UPDT_MERGE({update, [{update, F, {add, 2}}]}, b, B2),
%    A4 = merge(A3, B3),
%    Final = merge(A4, B3),
%    ?assertEqual([{F, [1,2]}], value(Final)).

%dot_key_test() ->
%    {ok, A} = ?UPDT_MERGE({update, [{update, {'X', riak_dt_orswot}, {add, <<"a">>}}, {update, {'X', riak_dt_od_flag}, enable}]}, a, new()),
%    B = A,
%    {ok, A2} = ?UPDT_MERGE({update, [{remove, {'X', riak_dt_od_flag}}]}, a, A),
%    ?assertEqual([{{'X', riak_dt_orswot}, [<<"a">>]}], value(merge(B, A2))).

-ifdef(EQC).
-define(NUMTESTS, 1000).

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

%% Add depth parameter
gen_op() ->
    ?SIZED(Size, gen_op(Size)).

gen_op(Size) ->
    ?LET(Ops, non_empty(list(gen_update(Size))), {update, Ops}).

gen_update(Size) ->
    ?LET(Field, gen_field(Size),
         oneof([{remove, Field},
                {update, Field, gen_field_op(Field, Size div 2)}])).

gen_field() ->
    ?SIZED(Size, gen_field(Size)).

gen_field(Size) ->
    {growingelements(['A', 'B', 'C', 'X', 'Y', 'Z']) %% Macro? Bigger?
    , elements([
                riak_dt_emcntr,
                riak_dt_orswot,
%%                riak_dt_lwwreg,
                riak_dt_od_flag
               ] ++ [?MODULE || Size > 0])}.

gen_field_op({_Name, Type}, Size) ->
    Type:gen_op(Size).


-endif.

-endif.
