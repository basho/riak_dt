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
-export([update/3, merge/2]).
-export([delta_update/3]).

-compile(export_all).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DICT, orddict).

new() ->
    {riak_dt_vclock:fresh(), orddict:new(), orddict:new(), ?DICT:new()}.

value({Clock, Entries, _Seen, _Deferred}) ->
    [K || {K, Dots} <- orddict:to_list(Entries),
        riak_dt_vclock:subtract_dots(Dots, Clock) == []].

update({add, Elem}, Actor, ORSet) ->
    {ok, add_elem(Actor, ORSet, Elem)};
update({remove, Elem}, _Actor, ORSet) ->
    {_Clock, Entries, _Seen, _Deferred} = ORSet,
    remove_elem(orddict:find(Elem, Entries), Elem, ORSet).

add_elem(Dot, {Clock, Entries, Seen, _Deferred}, Elem) when is_tuple(Dot) ->
    {riak_dt_vclock:merge([Clock, [Dot]]), orddict:store(Elem, [Dot], Entries), Seen, _Deferred};
add_elem(Actor, {Clock, Entries, Seen, _Deferred}, Elem) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = [{Actor, riak_dt_vclock:get_counter(Actor, NewClock)}],
    {NewClock, orddict:store(Elem, Dot, Entries), Seen, _Deferred}.

remove_elem({ok, _VClock}, Elem, {Clock, Dict, Seen, _Deferred}) ->
    {ok, {Clock, orddict:erase(Elem, Dict), Seen}, _Deferred};
remove_elem(_, Elem, _ORSet) ->
    {error, {precondition, {not_present, Elem}}}.

delta_update({add, Elem}, Actor, {Clock, _Entries, _Seen}) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Counter = riak_dt_vclock:get_counter(Actor, NewClock),
    Dot = [{Actor, Counter}],
    {ok, {[], orddict:store(Elem, Dot, orddict:new()), Dot, ?DICT:new()}};
delta_update({remove, Elem}, _Actor, {_Clock, Entries, _Seen}) ->
    case orddict:find(Elem, Entries) of
        {ok, Dots} ->
            {ok, {[], [], Dots, ?DICT:new()}};
        error ->
            {ok, {[], [], orddict:new(), ?DICT:new()}}
    end.

delta_update({add, Elem}, Actor, {Clock, _Entries, _Seen, _Deferred}, _Ctx) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Counter = riak_dt_vclock:get_counter(Actor, NewClock),
    Dot = [{Actor, Counter}],
    {ok, {[], orddict:store(Elem, Dot, orddict:new()), Dot, ?DICT:new()}};
delta_update({remove, Elem}, _Actor, {Clock, Entries, _Seen, _Deferred}, Ctx) ->
    Deferred = defer_remove(Clock, Ctx, Elem, ?DICT:new()),
    case orddict:find(Elem, Entries) of
        {ok, Dots} ->
            {ok, {[], [], Dots, Deferred}};
        error ->
            {ok, {[], [], [], Deferred}}
    end.

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

merge({LHClock, LHEntries, LHSeen, LHDeferred}=LHS, {RHClock, RHEntries, RHSeen, RHDeferred}=RHS) ->
    Clock0 = riak_dt_vclock:merge([LHClock, RHClock]),

    LHKeys = sets:from_list(orddict:fetch_keys(LHEntries)),
    RHKeys = sets:from_list(orddict:fetch_keys(RHEntries)),
    CommonKeys = sets:intersection(LHKeys, RHKeys),
    LHUnique = sets:subtract(LHKeys, CommonKeys),
    RHUnique = sets:subtract(RHKeys, CommonKeys),
    Entries00 = merge_common_keys(CommonKeys, LHS, RHS),

    Entries0 = merge_disjoint_keys(LHUnique, LHEntries, RHClock, RHSeen, Entries00),
    Entries1 = merge_disjoint_keys(RHUnique, RHEntries, LHClock, LHSeen, Entries0),

    Seen0 = lists:umerge(LHSeen, RHSeen),
    {Clock, Seen} = compress_seen(Clock0, Seen0),

    DeferredLR =  ?DICT:merge(fun(_Key, V1, V2) ->
                                     lists:umerge(V1,V2)
                             end, LHDeferred,RHDeferred),

    Entries = apply_deferred(Entries1, DeferredLR),
    Deferred =  clear_deferred(DeferredLR, Clock),

    {Clock, Entries, Seen, Deferred}.

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

%Reuse the interface operations instead of clearing the dots?
apply_deferred(Entries, Deferred) ->
    ?DICT:fold(fun(Ctx, Elems, EntriesAcc0) ->
                       lists:foldl(fun(Elem, EntriesAcc) -> 
                                           case ?DICT:find(Elem, EntriesAcc) of
                                               {ok, Dots} ->
                                                   case riak_dt_vclock:subtract_dots(Dots, Ctx) of
                                                       [] -> ?DICT:erase(Elem, EntriesAcc);
                                                       Remaining -> ?DICT:store(Elem, Remaining, EntriesAcc)
                                                   end;
                                               error -> EntriesAcc
                                           end
                                   end, EntriesAcc0, Elems)
               end,
               Entries,
               Deferred).

clear_deferred(Deferred, Clock) ->
    [ {Ctx, Elem} || {Ctx, Elem} <- ?DICT:to_list(Deferred), not riak_dt_vclock:descends(Clock, Ctx)].

%% @doc check if each element in `Entries' should be in the merged
%% set.
merge_disjoint_keys(Keys, Entries, SetClock, SetSeen, Accumulator) ->
    sets:fold(fun(Key, Acc) ->
                      Dots = orddict:fetch(Key, Entries),
                      case riak_dt_vclock:descends(SetClock, Dots) of
                          false ->
                              %% Optimise the set of stored dots to
                              %% include only those unseen
                              NewDots = riak_dt_vclock:subtract_dots(Dots, SetClock),
                              case lists:subtract(NewDots, SetSeen) of
                                  [] ->
                                      Acc;
                                  NewDots2 ->
                                      orddict:store(Key, lists:usort(NewDots2), Acc)
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
                      V1 = orddict:fetch(Key, LHSEntries),
                      V2 = orddict:fetch(Key, RHSEntries),

                      CommonDots = sets:intersection(sets:from_list(V1), sets:from_list(V2)),
                      LHSUnique = sets:to_list(sets:subtract(sets:from_list(V1), CommonDots)),
                      RHSUnique = sets:to_list(sets:subtract(sets:from_list(V2), CommonDots)),
                      LHSKeep = lists:subtract(riak_dt_vclock:subtract_dots(LHSUnique, RHSClock), RHSeen),
                      RHSKeep = lists:subtract(riak_dt_vclock:subtract_dots(RHSUnique, LHSClock), LHSeen),
                      V = riak_dt_vclock:merge([sets:to_list(CommonDots), LHSKeep, RHSKeep]),
                      %% Perfectly possible that an item in both sets should be dropped
                      case V of
                          [] ->
                              orddict:erase(Key, Acc);
                          _ ->
                              orddict:store(Key, lists:usort(V), Acc)
                      end
              end,
              orddict:new(),
              CommonKeys).


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
    {ok, {CtxA1,_,_,_} = D_A1} = ?ADD(a, a, InitialState),
    {ok, D_A2} = ?REM(a, a, D_A1, CtxA1),
    A2 = merge(D_A2, merge(D_A1, InitialState)),
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
    


-endif.
