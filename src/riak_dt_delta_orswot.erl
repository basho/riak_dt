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


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% API
-export([new/0, value/1, value/2]).
-export([update/3, merge/2]).
-export([delta_update/3]).

-compile(export_all).

new() ->
    {riak_dt_vclock:fresh(), orddict:new(), orddict:new()}.

value({_Clock, Entries, _Seen}) ->
    [K || {K, _Dots} <- orddict:to_list(Entries)].

value(pec, {_C, E, _S}) ->
    E.

update({add, Elem}, Actor, ORSet) ->
    {ok, add_elem(Actor, ORSet, Elem)};
update({remove, Elem}, _Actor, ORSet) ->
    {_Clock, Entries, _Deferred} = ORSet,
    remove_elem(orddict:find(Elem, Entries), Elem, ORSet).

add_elem(Dot, {Clock, Entries, Seen}, Elem) when is_tuple(Dot) ->
    {riak_dt_vclock:merge([Clock, [Dot]]), orddict:store(Elem, [Dot], Entries), Seen};
add_elem(Actor, {Clock, Entries, Seen}, Elem) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = [{Actor, riak_dt_vclock:get_counter(Actor, NewClock)}],
    {NewClock, orddict:store(Elem, Dot, Entries), Seen}.

remove_elem({ok, _VClock}, Elem, {Clock, Dict, Seen}) ->
    {ok, {Clock, orddict:erase(Elem, Dict), Seen}};
remove_elem(_, Elem, _ORSet) ->
    {error, {precondition, {not_present, Elem}}}.

delta_update({add, Elem}, Actor, {Clock, _Entries, _Seen}) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Counter = riak_dt_vclock:get_counter(Actor, NewClock),
    Dot = [{Actor, Counter}],
    %% @TODO what about this "optimisation" that means _more_ than a
    %% delta is sent (it changes how removes work at another replica
    %% profoundly!) For example if element 'X' exists with dots
    %% a1,b4,d6 and we add it at a3, if we treat that add as a remove
    %% of a1,b4,d6 and send them in the delta, a remove of 'X' a3 at
    %% replica Z will not remove a1,b4,d6, even though _seeing_ a3
    %% implies Z has seen what a3 supercedes.  BUT, if you're adding
    %% 'X' with "action at a distance" then that presupposes you
    %% _haven't_ seen 'X' (why would you add it if you had) which
    %% makes the remove optimisation incorrect! What if you send a
    %% context for adds?
    {C, S} = {[], []},%%prev_ctx(Elem, Entries),
    {Clock2, Seen2} = compress_seen(C, lists:umerge(Dot, S)),
    {ok, {Clock2, orddict:store(Elem, Dot, orddict:new()), Seen2}};
delta_update({remove, Elem}, _Actor, {_Clock, Entries, _Seen}) ->
    case orddict:find(Elem, Entries) of
        {ok, Dots} ->
             {Clock2, Seen2} = compress_seen([], Dots),
            {ok, {Clock2, [], Seen2}};
        error ->
            {ok, {[], [], []}}
    end;
%% A bigset style remove, where the elements dots are sent as ctx to
%% the client
delta_update({remove, _Elem, Ctx}, _Actor, {_Clock, _Entries, _Seen}) ->
    {Clock2, Seen2} = compress_seen([], Ctx),
    {ok, {Clock2, [], Seen2}}.


prev_ctx(Elem, Entries) ->
    case orddict:find(Elem, Entries) of
        error ->
            {[], []};
        {ok, Dots} ->
            compress_seen([], Dots)
    end.

merge({LHClock, LHEntries, LHSeen}=LHS, {RHClock, RHEntries, RHSeen}=RHS) ->
    Clock0 = riak_dt_vclock:merge([LHClock, RHClock]),

    LHKeys = sets:from_list(orddict:fetch_keys(LHEntries)),
    RHKeys = sets:from_list(orddict:fetch_keys(RHEntries)),
    CommonKeys = sets:intersection(LHKeys, RHKeys),
    LHUnique = sets:subtract(LHKeys, CommonKeys),
    RHUnique = sets:subtract(RHKeys, CommonKeys),
    Entries00 = merge_common_keys(CommonKeys, LHS, RHS),

    Entries0 = merge_disjoint_keys(LHUnique, LHEntries, RHClock, RHSeen, Entries00),
    Entries = merge_disjoint_keys(RHUnique, RHEntries, LHClock, LHSeen, Entries0),

    Seen0 = lists:umerge(LHSeen, RHSeen),
    {Clock, Seen} = compress_seen(Clock0, Seen0),
    {Clock, Entries, Seen}.

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
merge_common_keys(CommonKeys, {LHSClock, LHSEntries, LHSeen}, {RHSClock, RHSEntries, RHSeen}) ->

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



-ifdef(TEST).
out_of_order_test() ->
    A=B = new(),
    {ok, D1} = delta_update({add, 'x'}, a, A),
    A2 = merge(A, D1),
    {ok, D2} = delta_update({add, 'x'}, a, A2),
    A3 = merge(A2, D2),
    {ok, D3} = delta_update({remove, 'x'}, a, A3),
    A4 = merge(A3, D3),

    %% no matter what order those three deltas reach B, the result is
    %% the same
    ?assertEqual(A4, merge(D1, merge(merge(B, D2), D3))),
    ?assertEqual(A4, merge(D1, merge(merge(B, D3), D2))),
    ?assertEqual(A4, merge(D2, merge(merge(B, D1), D3))),
    ?assertEqual(A4, merge(D2, merge(merge(B, D3), D1))),
    ?assertEqual(A4, merge(D3, merge(merge(B, D1), D2))),
    ?assertEqual(A4, merge(D3, merge(merge(B, D2), D1))).

-endif.
