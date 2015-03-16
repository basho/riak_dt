%% -------------------------------------------------------------------
%%
%% riak_dt_delta_orswot: Tombstone-less, replicated, state based
%% observe remove set, with delta mutators.
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
-module(riak_dt_delta_orswot).

%% API
-export([new/0, value/1]).
-export([update/3, update/4, merge/2]).
-export([delta_update/3, delta_update/4]).
-export([precondition_context/1, parent_clock/2, get_deferred/1]).
-export([to_binary/1, to_binary/2, from_binary/1]).


-opaque delta_orswot() :: {riak_dt_vclock:vclock(), entries(), seen(), deferred()}.
%% Only removes can be deferred, so a list of members to be removed
%% per context.
-type seen() :: dots().
-type deferred() :: [{riak_dt_vclock:vclock(), [member()]}].
-type binary_orswot() :: binary(). %% A binary that from_binary/1 will operate on.

-type orswot_op() ::  {add, member()} | {remove, member()} |
                      {add_all, [member()]} | {remove_all, [member()]} |
                      {update, [orswot_op()]}.

-type actor() :: riak_dt:actor().

%% a dict of member() -> minimal_clock() mappings.  The
%% `minimal_clock()' is a more effecient way of storing knowledge
%% about adds / removes than a UUID per add.
-type entries() :: [{member(), minimal_clock()}].

%% a minimal clock is just the dots for the element, each dot being an
%% actor and event counter for when the element was added.
-type minimal_clock() :: [dot()].
-type dot() :: riak_dt:dot().
-type dots() :: [dot()].
-type member() :: term().

-type precondition_error() :: {error, {precondition ,{not_present, member()}}}.

-export_type([delta_orswot/0, orswot_op/0, binary_orswot/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DICT, dict).

-spec new() -> delta_orswot().
new() ->
    {riak_dt_vclock:fresh(), ?DICT:new(), ordsets:new(), ?DICT:new()}.

-spec value(delta_orswot()) -> [member()].
value({Clock, Entries, _Seen, _Deferred}) ->
    [K || {K, Dots} <- ?DICT:to_list(Entries),
        riak_dt_vclock:subtract_dots(Dots, Clock) == []].

-spec update(orswot_op(), actor() | dot(), delta_orswot()) -> {ok, delta_orswot()} |
                                                precondition_error().
update({add, Elem}, Actor, ORSet) ->
    add_elem(Actor, ORSet, Elem);
update({remove, Elem}, _Actor, ORSet) ->
    {_Clock, Entries, _Seen, _Deferred} = ORSet,
    remove_elem(?DICT:find(Elem, Entries), Elem, ORSet);
update({remove_all, Elems}, Actor, ORSet) ->
    remove_all(Elems, Actor, ORSet);
update({add_all, Elems}, Actor, ORSet) ->
    ORSet2 = lists:foldl(fun(E, S) ->
                                 {ok, ORSet1} = add_elem(Actor, S, E),
                                 ORSet1
                         end,
                         ORSet,
                         Elems),
    {ok, ORSet2};
update({update, Ops}, Actor, ORSet) ->
    apply_ops(Ops, Actor, ORSet).

add_elem(ActorOrDot, {Clock, Entries, Seen, _Deferred}, Elem) ->
    {Dot, NewClock} = update_clock(ActorOrDot, Clock),
    {ok, {NewClock, ?DICT:store(Elem, [Dot], Entries), Seen, _Deferred}}.

remove_elem({ok, _VClock}, Elem, {Clock, Dict, Seen, _Deferred}) ->
    {ok, {Clock, ?DICT:erase(Elem, Dict), Seen, _Deferred}};

remove_elem(_, Elem, _ORSet) ->
    {error, {precondition, {not_present, Elem}}}.

remove_all([], _Actor, ORSet) ->
    {ok, ORSet};

remove_all([Elem | Rest], Actor, ORSet) ->
    case update({remove, Elem}, Actor, ORSet) of
        {ok, ORSet2} ->
            remove_all(Rest, Actor, ORSet2);
        Error ->
            Error
    end.

-spec update(orswot_op(), actor() | dot(), delta_orswot(), riak_dt:context()) ->
    {ok, delta_orswot()} | precondition_error().
update({add, Elem}, ActorOrDot, ORSet, _Ctx) ->
    add_elem(ActorOrDot, ORSet, Elem);

update({remove, Elem}, _Actor, ORSet, Ctx) ->
    {_Clock, Entries, _Seen, _Deferred} = ORSet,
    remove_elem(?DICT:find(Elem, Entries), Elem, ORSet, Ctx);

update({remove_all, Elems}, Actor, ORSet, Ctx) ->
    remove_all(Elems, Actor, ORSet, Ctx);

update({add_all, Elems}, ActorOrDot, ORSet, _Ctx) ->
    update({add_all, Elems}, ActorOrDot, ORSet);

update({update, Ops}, ActorOrDot, ORSet, Ctx) ->
    ORSet2 = lists:foldl(fun(Op, Set) ->
                                 {ok, NewSet} = update(Op, ActorOrDot, Set, Ctx),
                                 NewSet
                         end,
                         ORSet,
                         Ops),
    {ok, ORSet2}.
-spec remove_all([orswot_op()], actor(), delta_orswot(), riak_dt:context()) -> {ok, delta_orswot} | precondition_error().
remove_all([], _Actor, ORSet, _Ctx) ->
    {ok, ORSet};

remove_all([Elem | Rest], Actor, ORSet, Ctx) ->
    case update({remove, Elem}, Actor, ORSet, Ctx) of
        {ok, ORSet2} -> remove_all(Rest, Actor, ORSet2, Ctx);
        {error, _} -> remove_all(Rest, Actor, ORSet, Ctx)
    end.

remove_elem({ok, Dots}, Elem, {Clock, Dict, Seen, Deferred0}, Ctx) ->
    Deferred = defer_remove(Clock, Ctx, Elem, Deferred0),
    DictUpdt = case riak_dt_vclock:subtract_dots(Dots, Ctx) of
                   [] -> ?DICT:erase(Elem, Dict);
                   Remaining -> ?DICT:store(Elem, Remaining, Dict)
               end,
    {ok, {Clock, DictUpdt, Seen, Deferred}};

remove_elem(_, Elem, {Clock, _Dict, _Seen, Deferred0}, Ctx) ->
    case defer_remove(Clock, Ctx, Elem, Deferred0) of
        Deferred0 ->
            {error, {precondition, {not_present, Elem}}};
        Deferred ->
            {ok, {Clock, _Dict, _Seen, Deferred}}
    end.

-spec apply_ops([orswot_op], actor() | dot(), delta_orswot()) ->
                       {ok, delta_orswot()} | precondition_error().
apply_ops([], _Actor, ORSet) ->
    {ok, ORSet};
apply_ops([Op | Rest], ActorOrDot, ORSet) ->
    case update(Op, ActorOrDot, ORSet) of
        {ok, ORSet2} ->
            apply_ops(Rest, ActorOrDot, ORSet2);
        Error ->
            Error
    end.

-spec delta_update(orswot_op(), actor() | dot(), delta_orswot(), riak_dt:context()) ->
    {ok, delta_orswot()} | {error, not_implemented}.
delta_update({add, Elem}, Actor, {Clock, _Entries, _Seen, _Def}=ORSet) ->
    {Dot, _NewClock} = update_clock(Actor, Clock),
    {ok, Delta} = delta_add_elem(Elem, Dot, new()),
    {ok, Update} = add_elem(Dot, ORSet, Elem),
    {ok, {Update, Delta}};
    %delta_add_elem(Elem, Dot, new());

delta_update({remove, Elem}, _Actor, {_, Entries, _, _}=ORSet) ->
    {ok, Delta} = delta_remove_elem(Elem, ORSet, new()),
    {ok, ORSet} = remove_elem(?DICT:find(Elem, Entries), Elem, ORSet),
    {ok, {ORSet, Delta}};
    %delta_remove_elem(Elem, ORSet, new());

delta_update({remove_all, Elems}, Actor, ORSet0) ->
    Delta = lists:foldl(fun(Elem , ORSetAcc) ->
                                {ok, ORSetAcc2} = delta_remove_elem(Elem, ORSet0, ORSetAcc),
                                ORSetAcc2
                        end, new(), Elems),
    {ok, ORSet} = remove_all(Elems, Actor, ORSet0),
    {ok, {ORSet, Delta}};
    %{ok, Delta};

%Batches of operations do not compose with the map... because operations have to reuse map's dot.
%Must add what elements were removed to the seen to make it work with just one dot.
delta_update({add_all, _Elems}, Actor, {_Clock0, _Entries, _Seen, _Deferred}) when is_tuple(Actor)->
    {error, not_implemented};

delta_update({add_all, Elems}=Op, Actor, {Clock0, _Entries, _Seen, _Deferred}=Obj) ->
    {_, DeltaORSet} = lists:foldl(fun(Elem , {Clock, ORSetAcc}) ->
                                     {Dot, NewClock} = update_clock(Actor, Clock),
                                     {ok, Delta} = delta_add_elem(Elem, Dot, ORSetAcc),
                                     {NewClock, Delta}
                             end, {Clock0, new()}, Elems),
    {ok, ORSet} = update(Op, Actor,Obj),
    {ok, {ORSet,DeltaORSet}};
    %{ok, DeltaORSet};

delta_update({update, Ops}, ActorOrDot, ORSet0) ->
    ORSet = lists:foldl(fun(Op, Set) ->
                                {ok, NewSet} = delta_update(Op, ActorOrDot, ORSet0),
                                merge(Set, NewSet)
                        end,
                        new(),
                        Ops),
    {ok, ORSet}.


delta_update({add, Elem}, ActorOrDot, {Clock, _Entries, _Seen, _Deferred}=ORSet, _Ctx) ->
    {Dot, _} = update_clock(ActorOrDot, Clock),
    {ok, Delta} = delta_add_elem(Elem, Dot, new()),
    {ok, Update} = add_elem(Dot, ORSet, Elem),
    {ok, {Update, Delta}};
    %delta_add_elem(Elem, Dot, new());


delta_update({add_all, _Elems}, Actor, {_Clock0, _Entries, _Seen, _Deferred}, _Ctx) when is_tuple(Actor) ->
    {error, not_implemented};

delta_update({add_all, Elems}=Op, ActorOrDot, {Clock0, _Entries, _Seen, _Deferred}=Obj, _Ctx) ->
    {_, DeltaORSet} = lists:foldl(fun(Elem , {Clock, DeltaAccIn}) ->
                                     {Dot, NewClock} = update_clock(ActorOrDot, Clock),
                                     {ok, DeltaAcc} = delta_add_elem(Elem, Dot, DeltaAccIn),
                                     {NewClock, DeltaAcc}
                             end, {Clock0, new()}, Elems),
    {ok, ORSet} = update(Op, ActorOrDot,Obj),
    {ok, {ORSet, DeltaORSet}};
    %{ok, DeltaORSet};

delta_update({remove, Elem}, _Actor, {_, Entries, _, _}=ORSet0, Ctx) ->
    {ok, Delta} = delta_remove_elem(Elem, ORSet0, new(), Ctx),
    {ok, ORSet} = remove_elem(?DICT:find(Elem, Entries), Elem, ORSet0, Ctx),
    {ok, {ORSet, Delta}};
    %delta_remove_elem(Elem, ORSet0, new(), Ctx);

delta_update({remove_all, Elems}, Actor, ORSet0, Ctx) ->
    DeltaORSet = lists:foldl(fun(Elem , ORSetAcc) ->
                                {ok, ORSetAcc2} = delta_remove_elem(Elem, ORSet0, ORSetAcc, Ctx),
                                ORSetAcc2
                        end, new(), Elems),
    {ok, ORSet} = remove_all(Elems, Actor, ORSet0, Ctx),
    {ok, {ORSet, DeltaORSet}};
    %{ok, DeltaORSet};

%TODO: This is not efficient
delta_update({update, Ops}, ActorOrDot, ORSet0, Ctx) ->
    ORSet = lists:foldl(fun(Op, Set) ->
                                {ok, NewSet} = delta_update(Op, ActorOrDot, ORSet0, Ctx),
                                merge(Set, NewSet)
                        end,
                        new(),
                        Ops),
    {ok, ORSet}.

-spec delta_add_elem(orswot_op(), dot(), delta_orswot()) -> {ok, delta_orswot()}.
delta_add_elem(Elem, Dot, {_Clock, Entries, Seen, _Deferred}) ->
    {ok, {[], ?DICT:store(Elem, [Dot], Entries), lists:umerge([Dot], Seen), _Deferred}}.

-spec delta_remove_elem(orswot_op(), delta_orswot(), delta_orswot()) -> {ok, delta_orswot()}.
delta_remove_elem(Elem, {_, Entries, _, _}, {_, _, SeenOut, _DeferredOut}) ->
    case ?DICT:find(Elem, Entries) of
        {ok, Dots} ->
            {ok, {[], [], ordsets:union(Dots,SeenOut), _DeferredOut}};
        error ->
            {ok, {[], [], SeenOut, _DeferredOut}}
    end.

-spec delta_remove_elem(orswot_op(), delta_orswot(), delta_orswot(), riak_dt_vclock:vclock()) -> {ok, delta_orswot()}.
%delta_remove_elem(Elem, {Clock, Entries, _, _}, {_, _EntriesOut, _SeenOut, DeferredOut}, Ctx) ->
%    Deferred0 = defer_remove(Clock, Ctx, Elem, DeferredOut),
%    Deferred = case ?DICT:size(Deferred0) > 0 of
%                   true -> Deferred0;
%                   false ->
%                       case ?DICT:find(Elem, Entries) of
%                           {ok, Dots} ->
%                              THIS IS NOT COMPLETE
%                               Remaining = riak_dt_vclock:subtract_dots(Dots, Ctx),
%                               Removed = lists:subtract(Dots, Remaining),
%                               ?DICT:update(Removed, fun(Elems) ->
%                                                          ordset:add_element(Elem, Elems)
%                                                  end, ordsets:add_element(Elem, ordsets:new()), DeferredOut);
%                           _ -> %Should not enter here
%                               defer_remove(Clock, Ctx, Elem, DeferredOut)
%                      end
%               end,
%    {ok, {[], ?DICT:new(), [], Deferred}}.

delta_remove_elem(Elem, {_Clock, _Entries, _, _}, {_, _EntriesOut, _SeenOut, DeferredOut}, Ctx) ->
    Deferred0 = always_defer_remove(Ctx, Elem, DeferredOut),
    {ok, {[], ?DICT:new(), [], Deferred0}}.


always_defer_remove(Ctx, Elem, Deferred) ->
    ?DICT:update(Ctx,
                 fun(Elems) ->
                         ordsets:add_element(Elem, Elems) end,
                 ordsets:add_element(Elem, ordsets:new()),
                 Deferred).

-spec defer_remove(riak_dt_vclock:vclock(), riak_dt_vclock:vclock(), orswot_op(), deferred()) ->
                      deferred().
defer_remove(Clock, Ctx, Elem, Deferred) ->
    case riak_dt_vclock:descends(Clock, Ctx) of
        %% no need to save this remove, we're done
        true -> Deferred;
        false -> ?DICT:update(Ctx,
                                fun(Elems) ->
                                        ordsets:add_element(Elem, Elems) end,
                                ordsets:add_element(Elem, ordsets:new()),
                                Deferred)
    end.

-spec merge(delta_orswot(), delta_orswot()) -> delta_orswot().
%%Delta merge --- assumes deltas are not merged. Might not be ready for removes
merge({[], LHEntries, LHSeen, LHDeferred}=_LHS, {RHClock, RHEntries, RHSeen, RHDeferred}=_RHS) ->
    Entries = ?DICT:fold(fun(Elem, LHDots, EntriesAcc) ->
                                 ?DICT:update(Elem, fun(Dots) ->
                                                            lists:umerge(LHDots, Dots)
                                                    end, LHDots, EntriesAcc)
                         end, RHEntries, LHEntries),
    Seen0 = lists:umerge(LHSeen, RHSeen),
    {Clock, Seen} = compress_seen(RHClock, Seen0),
    Deferred = ?DICT:merge(fun(_Key, V1, V2) ->
                                   lists:umerge(V1,V2)
                           end, LHDeferred,RHDeferred),
    apply_deferred({Clock, Entries, Seen, Deferred});

merge(LHCRDT, {[], _, _, _RHDeferred}=RHCRDT) ->
    merge(RHCRDT, LHCRDT);

merge({LHClock, LHEntries, LHSeen, LHDeferred}=LHS, {RHClock, RHEntries, RHSeen, RHDeferred}=RHS) ->
    Clock0 = riak_dt_vclock:merge([LHClock, RHClock]),
    LHKeys = sets:from_list(?DICT:fetch_keys(LHEntries)),
    RHKeys = sets:from_list(?DICT:fetch_keys(RHEntries)),
    CommonKeys = sets:intersection(LHKeys, RHKeys),
    LHUnique = sets:subtract(LHKeys, CommonKeys),
    RHUnique = sets:subtract(RHKeys, CommonKeys),
    Entries00 = merge_common_keys(CommonKeys, LHS, RHS),
    Entries0 = merge_disjoint_keys(LHUnique, LHEntries, RHClock, RHSeen, Entries00),
    Entries1 = merge_disjoint_keys(RHUnique, RHEntries, LHClock, LHSeen, Entries0),
    Seen0 = lists:umerge(LHSeen,RHSeen),
    {Clock, Seen} = compress_seen(Clock0, Seen0),
    Deferred = ?DICT:merge(fun(_Key, V1, V2) ->
                                     lists:umerge(V1,V2)
                             end, LHDeferred,RHDeferred),
    apply_deferred({Clock, Entries1, Seen, Deferred}).

-spec compress_seen(riak_dt:vclock(), seen()) -> {riak_dt_vclock:vclock(), seen()}.
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

apply_deferred({Clock, Entries, Seen, Deferred}) ->
    ?DICT:fold(fun(Ctx, Elems, ORSetAcc) ->
                                               {ok, ORSet} = update({remove_all, Elems}, undefined, ORSetAcc, Ctx),
                       ORSet
               end,
               {Clock, Entries, Seen, dict:new()},
               Deferred).

%% @doc check if each element in `Entries' should be in the merged
%% set.
merge_disjoint_keys(Keys, Entries, SetClock, SetSeen, Accumulator) ->
    sets:fold(fun(Key, Acc) ->
                      Dots = ?DICT:fetch(Key, Entries),
                      case riak_dt_vclock:descends(SetClock, Dots) of
                          false ->
                              %% Optimise the set of stored dots to
                              %% include only those unseen
                              NewDots = riak_dt_vclock:subtract_dots(Dots, SetClock),
                              case lists:subtract(NewDots, SetSeen) of
                                  [] ->
                                      Acc;
                                  NewDots2 ->
                                      ?DICT:store(Key, lists:usort(NewDots2), Acc)
                              end;
                          true ->
                              Acc
                      end
              end,
              Accumulator,
              Keys).

%% @doc merges the minimal clocks for the common entries in both sets.
merge_common_keys(CommonKeys, {LHSClock, LHSEntries, LHSeen, _LHDef}, {RHSClock, RHSEntries, RHSeen, _RHDef}) ->

    %% If both sides have the same values, some dots may still need to
    %% be shed.  If LHS has dots for 'X' that RHS does _not_ have, and
    %% RHS's clock dominates those dots, then we need to drop those
    %% dots.  We only keep dots BOTH side agree on, or dots that are
    %% not dominated. Keep only common dots, and dots that are not
    %% dominated by the other sides clock

    sets:fold(fun(Key, Acc) ->
                      V1 = ?DICT:fetch(Key, LHSEntries),
                      V2 = ?DICT:fetch(Key, RHSEntries),

                      CommonDots = sets:intersection(sets:from_list(V1), sets:from_list(V2)),
                      LHSUnique = sets:to_list(sets:subtract(sets:from_list(V1), CommonDots)),
                      RHSUnique = sets:to_list(sets:subtract(sets:from_list(V2), CommonDots)),
                      LHSKeep = lists:subtract(riak_dt_vclock:subtract_dots(LHSUnique, RHSClock), RHSeen),
                      RHSKeep = lists:subtract(riak_dt_vclock:subtract_dots(RHSUnique, LHSClock), LHSeen),
                      V = riak_dt_vclock:merge([sets:to_list(CommonDots), LHSKeep, RHSKeep]),
                      %% Perfectly possible that an item in both sets should be dropped
                      case V of
                          [] ->
                              ?DICT:erase(Key, Acc);
                          _ ->
                              ?DICT:store(Key, lists:usort(V), Acc)
                      end
              end,
              ?DICT:new(),
              CommonKeys).

precondition_context({Clock, _Entries, _Seen, _Deferred}) ->
    Clock.

-spec parent_clock(riak_dt_vclock:vclock(), delta_orswot()) -> delta_orswot().
parent_clock(Clock, {_, Entries, Seen, Deferred}) -> {Clock, Entries, Seen, Deferred}.

-spec get_deferred(delta_orswot()) -> [riak_dt:context()].
get_deferred({_, _, _, Deferred}) ->
    lists:map(fun({Key, _}) -> Key end, ?DICT:to_list(Deferred)).

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



-include("riak_dt_tags.hrl").
-define(TAG, ?DT_ORSWOT_TAG).
-define(V1_VERS, 1).
-define(V2_VERS, 2).

%% @doc returns a binary representation of the provided
%% `orswot()'. The resulting binary is tagged and versioned for ease
%% of future upgrade. Calling `from_binary/1' with the result of this
%% function will return the original set. Use the application env var
%% `binary_compression' to turn t2b compression on (`true') and off
%% (`false')
%%
%% @see `from_binary/1'
to_binary(S) ->
    {ok, B} = to_binary(?V2_VERS, S),
    B.

%% @private encode v1 sets as v2, and vice versa. The first argument
%% is the target binary type.
to_binary(?V1_VERS, S0) ->
    S = to_v1(S0),
    {ok, <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(S))/binary>>};
to_binary(?V2_VERS, S0) ->
    S = to_v2(S0),
    {ok, <<?TAG:8/integer, ?V2_VERS:8/integer, (riak_dt:to_binary(S))/binary>>};
to_binary(Vers, _S0) ->
    ?UNSUPPORTED_VERSION(Vers).

%% @private transpose a v1 orswot (orddicts) to a v2 (dicts)
to_v2({Clock, Entries0, Deferred0}) when is_list(Entries0),
                                         is_list(Deferred0) ->
    %% Turn v1 set into a v2 set
    Entries = ?DICT:from_list(Entries0),
    Deferred = ?DICT:from_list(Deferred0),
    {Clock, Entries, Deferred};
to_v2(S) ->
    S.

%% @private transpose a v2 orswot (dicts) to a v1 (orddicts)
to_v1({_Clock, Entries0, Deferred0}=S) when is_list(Entries0),
                                            is_list(Deferred0) ->
    S;
to_v1({Clock, Entries0, Deferred0}) ->
    %% Must be dicts, there is no is_dict test though
    %% should we use error handling as logic here??
    Entries = riak_dt:dict_to_orddict(Entries0),
    Deferred = riak_dt:dict_to_orddict(Deferred0),
    {Clock, Entries, Deferred}.

%% @doc When the argument is a `binary_orswot()' produced by
%% `to_binary/1' will return the original `orswot()'.
%%
%% @see `to_binary/1'
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    S = riak_dt:from_binary(B),
    %% Now upgrade the structure to dict from orddict
    {ok, to_v2(S)};
from_binary(<<?TAG:8/integer, ?V2_VERS:8/integer, B/binary>>) ->
    {ok, riak_dt:from_binary(B)};
from_binary(<<?TAG:8/integer, Vers:8/integer, _B/binary>>) ->
    ?UNSUPPORTED_VERSION(Vers);
from_binary(_B) ->
    ?INVALID_BINARY.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% Delta update tests

-define(ADD(Elem, Actor, Set), delta_update({'add',Elem}, Actor, Set, undefined)).
-define(REM(Elem, Actor, Set, Ctx), delta_update({'remove',Elem}, Actor, Set, Ctx)).


simple_add_test() ->
    InitialState = new(),
    {ok, D_A1} = ?ADD(a, a, InitialState),
    A1 = merge(D_A1, InitialState),
    ?assertEqual([a], value(A1)).

add_remove_test() ->
    InitialState = new(),
    {ok, D_A1} = ?ADD(a, a, InitialState),
    {CtxA1,_,_,_} = A1 = merge(D_A1, InitialState),
    {ok, D_A2} = ?REM(a, a, D_A1, CtxA1),
    A2 = merge(D_A2, A1),
    ?assertEqual([], value(A2)).

concurrent_add_test() ->
    InitialState = new(),
    {ok, {_CtxA1,_,_,_} = D_A1} = ?ADD(a, a, InitialState),
    {ok, {_CtxB1,_,_,_} = D_B1} = ?ADD(b, b, InitialState),
    AB = merge(D_B1, merge(D_A1, InitialState)),
    ?assertEqual([a, b], value(AB)).

deferred_remove_test() ->
    InitialState = new(),
    {ok, D_A1} = ?ADD(a, a, InitialState),
    {CtxA1,_,_,_} = MergedA1 = merge(D_A1,InitialState),
    {ok, {_CtxB1,_,_,_} = D_B1} = ?REM(a, b, InitialState, CtxA1),
    AB = merge(D_B1, merge(MergedA1, InitialState)),
    ?assertEqual([], value(AB)).
    
deferred_remove_concurrent_add_test() ->
    InitialState = new(),
    {ok, D_A1} = ?ADD(a, a, InitialState),
    {CtxA1,_,_,_} = MergedA1 = merge(D_A1,InitialState),
    {ok, D_A2} = ?ADD(a, a, MergedA1),
    {_CtxA2,_,_,_} = MergedA2 = merge(D_A2,MergedA1),
    {ok, {_CtxB1,_,_,_} = D_B1} = ?REM(a, b, InitialState, CtxA1),
    AB = merge(D_B1, merge(MergedA2, InitialState)),
    ?assertEqual([a], value(AB)).

deferred_remove_ctx_test() ->
    InitialState = new(),
    {ok, D_A1} = ?ADD(a, a, InitialState),
    {CtxA1,_,_,_} = merge(D_A1, InitialState),
    {ok, D_B1} = ?ADD(a, b, InitialState),
    {CtxB1,_,_,_} = MergedB1 = merge(D_B1, InitialState),
    {ok, D_B2} = ?REM(a, b, MergedB1, CtxA1),
    {_CtxB2, _, _, _}=B2 = merge(D_B2, MergedB1),
    ?assertEqual([a], value(B2)),
    A1B2 = merge(D_A1, B2),
    {ok, D_B3} = ?REM(a, b, A1B2, CtxB1),
    A1B3 = merge(D_B3, merge(A1B2, D_B3)),
    ?assertEqual([], value(A1B3)).

%% Added by @asonge from github to catch a bug I added by trying to
%% bypass merge if one side's clcok dominated the others. The
%% optimisation was bogus, this test remains in case someone else
%% tries that
disjoint_merge_test() ->
    {ok, A1} = update({add, <<"bar">>}, 1, new()),
    {ok, B1} = update({add, <<"baz">>}, 2, new()),
    C = merge(A1, B1),
    {ok, A2} = update({remove, <<"bar">>}, 1, A1),
    D = merge(A2, C),
    ?assertEqual([<<"baz">>], value(D)).

%% Bug found by EQC, not dropping dots in merge when an element is
%% present in both Sets leads to removed items remaining after merge.
present_but_removed_test() ->
    %% Add Z to A
    {ok, A} = update({add, 'Z'}, a, new()),
    %% Replicate it to C so A has 'Z'->{e, 1}
    C = A,
    %% Remove Z from A
    {ok, A2} = update({remove, 'Z'}, a, A),
    %% Add Z to B, a new replica
    {ok, B} = update({add, 'Z'}, b, new()),
    %%  Replicate B to A, so now A has a Z, the one with a Dot of
    %%  {b,1} and clock of [{a, 1}, {b, 1}]
    A3 = merge(B, A2),
    %% Remove the 'Z' from B replica
    {ok, B2} = update({remove, 'Z'}, b, B),
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
    {ok, A} = update({add, 'Z'}, a, new()),
    {ok, B} = update({add, 'Z'}, b, new()),
    C = A, %% replicate A to empty C
    {ok, A2} = riak_dt_delta_orswot:update({remove, 'Z'}, a, A),
    %% replicate B to A, now A has B's 'Z'
    A3 = riak_dt_delta_orswot:merge(A2, B),
    %% Remove B's 'Z'
    {ok, B2} = riak_dt_delta_orswot:update({remove, 'Z'}, b, B),
    %% Replicate C to B, now B has A's old 'Z'
    B3 = riak_dt_delta_orswot:merge(B2, C),
    %% Merge everytyhing, without the fix You end up with 'Z' present,
    %% with no dots
    Merged = lists:foldl(fun(Set, Acc) ->
                                 merge(Set, Acc) end,
                         A3,
                         [B3, C]),
    ?assertEqual([], value(Merged)).

%% A test I thought up
%% - existing replica of ['A'] at a and b,
%% - add ['B'] at b, but not communicated to any other nodes, context returned to client
%% - b goes down forever
%% - remove ['A'] at a, using the context the client got from b
%% - will that remove happen?
%%   case for shouldn't: the context at b will always be bigger than that at a
%%   case for should: we have the information in dots that may allow us to realise it can be removed
%%     without us caring.
%%
%% as the code stands, 'A' *is* removed, which is almost certainly correct. This behaviour should
%% always happen, but may not. (ie, the test needs expanding)
dead_node_update_test() ->
    {ok, A} = update({add, 'A'}, a, new()),
    {ok, B} = update({add, 'B'}, b, A),
    BCtx = precondition_context(B),
    {ok, A2} = update({remove, 'A'}, a, A, BCtx),
    ?assertEqual([], value(A2)).

%% Batching should not re-order ops
batch_order_test() ->
    {ok, Set} = update({add_all, [<<"bar">>, <<"baz">>]}, a, new()),
    Context  = precondition_context(Set),
    {ok, Set2} = update({update, [{remove, <<"baz">>}, {add, <<"baz">>}]}, a, Set, Context),
    ?assertEqual([<<"bar">>, <<"baz">>], value(Set2)),
    {ok, Set3} = update({update, [{remove, <<"baz">>}, {add, <<"baz">>}]}, a, Set),
    ?assertEqual([<<"bar">>, <<"baz">>], value(Set3)),
    {ok, Set4} = update({remove, <<"baz">>}, a, Set),
    {ok, Set5} = update({add, <<"baz">>}, a, Set4),
    ?assertEqual([<<"bar">>, <<"baz">>], value(Set5)).

batch_delta_order_test() ->
    InitialState = new(),
    {ok, Set_D} = delta_update({add_all, [<<"bar">>, <<"baz">>]}, a, InitialState),
    Set = merge(Set_D, InitialState),
    Context  = precondition_context(Set),
    {ok, Set2_D} = delta_update({update, [{remove, <<"baz">>}, {add, <<"baz">>}]}, a, Set, Context),
    Set2 = merge(Set2_D, Set),
    ?assertEqual([<<"bar">>, <<"baz">>], value(Set2)),
    {ok, Set3_D} = update({update, [{remove, <<"baz">>}, {add, <<"baz">>}]}, a, Set),
    Set3 = merge(Set3_D, Set),
    ?assertEqual([<<"bar">>, <<"baz">>], value(Set3)),
    {ok, Set4_D} = update({remove, <<"baz">>}, a, Set),
    Set4 = merge(Set4_D, Set),
    {ok, Set5_D} = update({add, <<"baz">>}, a, Set4),
    Set5 = merge(Set5_D, Set4),
    ?assertEqual([<<"bar">>, <<"baz">>], value(Set5)).

-endif.
