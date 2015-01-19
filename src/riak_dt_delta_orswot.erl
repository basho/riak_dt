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
-export([delta_update/3]).
-export([precondition_context/1]).

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
    add_elem(Actor, ORSet, Elem);
update({remove, Elem}, _Actor, ORSet) ->
    {_Clock, Entries, _Seen, _Deferred} = ORSet,
    remove_elem(orddict:find(Elem, Entries), Elem, ORSet);
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

add_elem(Dot, {Clock, Entries, Seen, _Deferred}, Elem) when is_tuple(Dot) ->
    {ok, {riak_dt_vclock:merge([Clock, [Dot]]), orddict:store(Elem, [Dot], Entries), Seen, _Deferred}};
add_elem(Actor, {Clock, Entries, Seen, _Deferred}, Elem) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = [{Actor, riak_dt_vclock:get_counter(Actor, NewClock)}],
    {ok, {NewClock, orddict:store(Elem, Dot, Entries), Seen, _Deferred}}.

remove_elem({ok, _VClock}, Elem, {Clock, Dict, Seen, _Deferred}) ->
    {ok, {Clock, orddict:erase(Elem, Dict), Seen, _Deferred}};
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

update({add, Elem}, Actor, ORSet, _Ctx) ->
    add_elem(Actor, ORSet, Elem);
update({remove, Elem}, _Actor, ORSet, Ctx) ->
    {_Clock, Entries, _Seen, _Deferred} = ORSet,
    remove_elem(orddict:find(Elem, Entries), Elem, ORSet, Ctx);
update({remove_all, Elems}, Actor, ORSet, Ctx) ->
    remove_all(Elems, Actor, ORSet, Ctx);
update({add_all, Elems}, Actor, ORSet, _Ctx) ->
    update({add_all, Elems}, Actor, ORSet);
update({update, Ops}, Actor, ORSet, Ctx) ->
    ORSet2 = lists:foldl(fun(Op, Set) ->
                                 {ok, NewSet} = update(Op, Actor, Set, Ctx),
                                 NewSet
                         end,
                         ORSet,
                         Ops),
    {ok, ORSet2}.

remove_all([], _Actor, ORSet, _Ctx) ->
    {ok, ORSet};
remove_all([Elem | Rest], Actor, ORSet, Ctx) ->
    {ok, ORSet2} =  update({remove, Elem}, Actor, ORSet, Ctx),
    remove_all(Rest, Actor, ORSet2, Ctx).

remove_elem({ok, Dots}, Elem, {Clock, Dict, Seen, Deferred0}, Ctx) ->
    Deferred = defer_remove(Clock, Ctx, Elem, Deferred0),
    DictUpdt = case riak_dt_vclock:subtract_dots(Dots, Ctx) of
                   [] -> orddict:erase(Elem, Dict);
                   Remaining -> orddict:store(Elem, Remaining, Dict)
               end,
    {ok, {Clock, DictUpdt, Seen, Deferred}};

remove_elem(_, Elem, {Clock, _Dict, _Seen, Deferred0}, Ctx) ->
    case defer_remove(Clock, Ctx, Elem, Deferred0) of
        Deferred0 -> 
            {error, {precondition, {not_present, Elem}}};
        Deferred ->
            {ok, {Clock, _Dict, _Seen, Deferred}}
    end.

apply_ops([], _Actor, ORSet) ->
    {ok, ORSet};
apply_ops([Op | Rest], Actor, ORSet) ->
    case update(Op, Actor, ORSet) of
        {ok, ORSet2} ->
            apply_ops(Rest, Actor, ORSet2);
        Error ->
            Error
    end.

delta_update({add, Elem}, Actor, {Clock, _Entries, _Seen}) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Counter = riak_dt_vclock:get_counter(Actor, NewClock),
    Delta = new(),
    Dot = [{Actor, Counter}],
    delta_add_elem(Elem, Dot, Delta);

delta_update({remove, Elem}, _Actor, ORSet) ->
    delta_remove_elem(Elem,ORSet, new());

delta_update({remove_all, Elems}, _Actor, ORSet) ->
    Delta = lists:foldl(fun(Elem , ORSetAcc) -> 
                                delta_remove_elem(Elem, ORSet, ORSetAcc)
                        end, new(), Elems),
    {ok, Delta}.

delta_update({add, Elem}, Actor, {Clock, _Entries, _Seen, _Deferred}, _Ctx) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Counter = riak_dt_vclock:get_counter(Actor, NewClock),
    Dot = [{Actor, Counter}],
    delta_add_elem(Elem, Dot, new());

delta_update({add_all, Elems}, Actor, {Clock, _Entries, _Seen, _Deferred}, _Ctx) ->
    Delta = lists:foldl(fun(Elem , ORSetAcc) -> 
                                NewClock = riak_dt_vclock:increment(Actor, Clock),
                                Counter = riak_dt_vclock:get_counter(Actor, NewClock),
                                Dot = [{Actor, Counter}],
                                delta_add_elem(Elem, Dot, ORSetAcc)
                        end, new(), Elems),
    {ok, Delta};

delta_update({remove, Elem}, _Actor, ORSet, Ctx) ->
    delta_remove_elem(Elem, ORSet, new(), Ctx);

delta_update({remove_all, Elems}, _Actor, ORSet, Ctx) ->
    Delta = lists:foldl(fun(Elem , ORSetAcc) -> 
                                delta_remove_elem(Elem, ORSet, ORSetAcc, Ctx)
                        end, new(), Elems),
    {ok, Delta};

delta_update({update, Ops}, Actor, ORSet, Ctx) ->
    ORSet2 = lists:foldl(fun(Op, Set) ->
                                 {ok, NewSet} = delta_update(Op, Actor, Set, Ctx),
                                 NewSet
                         end,
                         ORSet,
                         Ops),
    {ok, ORSet2}.

delta_add_elem(Elem, Dot, {_Clock, Entries, Seen, _Deferred}) ->
    {ok, {[], orddict:store(Elem, Dot, Entries), lists:append(Dot, Seen), _Deferred}}.

delta_remove_elem(Elem, {_, Entries, _, _}, {_, _, SeenOut, _DeferredOut}) ->
    case orddict:find(Elem, Entries) of
        {ok, Dots} ->
            {ok, {[], [], lists:append(Dots,SeenOut), _DeferredOut}};
        error ->
            {ok, {[], [], SeenOut, _DeferredOut}}
    end.

delta_remove_elem(Elem, {Clock, Entries, _, _}, {_, _, SeenOut, DeferredOut}, Ctx) ->
    Deferred = defer_remove(Clock, Ctx, Elem, DeferredOut),
    case orddict:find(Elem, Entries) of
        {ok, Dots} ->
            Remaining = riak_dt_vclock:subtract_dots(Dots, Ctx),
            {ok, {[], [], lists:append(Remaining, SeenOut), Deferred}};
        error ->
            {ok, {[], [], SeenOut, Deferred}}
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

    Deferred = ?DICT:merge(fun(_Key, V1, V2) ->
                                     lists:umerge(V1,V2)
                             end, LHDeferred,RHDeferred),

    apply_deferred({Clock, Entries1,Seen, Deferred}).

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
apply_deferred({_, _Entries, _Seen, Deferred}=ORSet0) ->
    ?DICT:fold(fun(Ctx, Elems, ORSetAcc) ->
                       {ok, ORSet} = update({remove_all, Elems}, undefined, ORSetAcc, Ctx),
                       ORSet
               end,
               ORSet0,
               Deferred).

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

precondition_context({Clock, _Entries, _Seen, _Deferred}) ->
    Clock.

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
    io:format("~p~n",[D_A1]),
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
    
deferred_remove_concurrent_add_test() ->
    InitialState = new(),
    {ok, D_A1} = ?ADD(a, a, InitialState),
    {CtxA1,_,_,_} = MergedA1 = merge(D_A1,InitialState),
    {ok, D_A2} = ?ADD(a, a, MergedA1),
    {_CtxA2,_,_,_} = MergedA2 = merge(D_A2,MergedA1),
    {ok, {_CtxB1,_,_,_} = D_B1} = ?REM(a, b, InitialState, CtxA1),
    AB = merge(D_B1, merge(MergedA2, InitialState)),
    ?assertEqual([a], value(AB)).

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

-endif.
