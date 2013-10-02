%% -------------------------------------------------------------------
%%
%% riak_core: Core Riak Application
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

%% @doc A simple Erlang implementation of vector clocks as inspired by Lamport logical clocks.
%%
%% @reference Leslie Lamport (1978). "Time, clocks, and the ordering of events
%% in a distributed system". Communications of the ACM 21 (7): 558-565.
%%
%% @reference Friedemann Mattern (1988). "Virtual Time and Global States of
%% Distributed Systems". Workshop on Parallel and Distributed Algorithms:
%% pp. 215-226

-module(riak_dt_vclock).

-export([fresh/0,descends/2,merge/1,get_counter/2, subtract_dots/2,
	increment/2,all_nodes/1, equal/2, replace_actors/2, replace_actors/3,
         to_binary/1, from_binary/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([vclock/0, vclock_node/0, binary_vclock/0]).

-type vclock() :: [vc_entry()].
-type binary_vclock() :: binary().
% The timestamp is present but not used, in case a client wishes to inspect it.
-type vc_entry() :: {vclock_node(), counter()}.

% Nodes can have any term() as a name, but they must differ from each other.
-type   vclock_node() :: term().
-type   counter() :: integer().

% @doc Create a brand new vclock.
-spec fresh() -> vclock().
fresh() ->
    [].

% @doc Return true if Va is a direct descendant of Vb, else false -- remember, a vclock is its own descendant!
-spec descends(Va :: vclock()|[], Vb :: vclock()|[]) -> boolean().
descends(_, []) ->
    % all vclocks descend from the empty vclock
    true;
descends(Va, Vb) ->
    [{NodeB, CtrB} |RestB] = Vb,
    case lists:keyfind(NodeB, 1, Va) of
        false ->
            false;
        {_, CtrA} ->
            (CtrA >= CtrB) andalso descends(Va,RestB)
    end.

%% @doc subtract the VClock from the DotList.
%% what this means is that any {actor(), count()} pair in
%% DotList that is <= an entry in  VClock is removed from DotList
%% Example [{a, 3}, {b, 2}, {d, 14}, {g, 22}] -
%%         [{a, 4}, {b, 1}, {c, 1}, {d, 14}, {e, 5}, {f, 2}] =
%%         [{{b, 2}, {g, 22}]
-spec subtract_dots(vclock(), vclock()) -> vclock().
subtract_dots(DotList, VClock) ->
    drop_dots(DotList, VClock, []).

drop_dots([], _Clock, NewDots) ->
    lists:sort(NewDots);
drop_dots([{Actor, Count}=Dot | Rest], Clock, Acc) ->
    case get_counter(Actor, Clock) of
        Cnt when Cnt >= Count ->
            %% Dot is dominated by clock, drop it
            drop_dots(Rest, Clock, Acc);
        _ ->
            drop_dots(Rest, Clock, [Dot | Acc])
    end.

% @doc Combine all VClocks in the input list into their least possible
%      common descendant.
-spec merge(VClocks :: [vclock()]) -> vclock() | [].
merge([])             -> [];
merge([SingleVclock]) -> SingleVclock;
merge([First|Rest])   -> merge(Rest, lists:keysort(1, First)).

merge([], NClock) -> NClock;
merge([AClock|VClocks],NClock) ->
    merge(VClocks, merge(lists:keysort(1, AClock), NClock, [])).

merge([], [], AccClock) -> lists:reverse(AccClock);
merge([], Left, AccClock) -> lists:reverse(AccClock, Left);
merge(Left, [], AccClock) -> lists:reverse(AccClock, Left);
merge(V=[{Node1, Ctr1}=NCT1|VClock],
      N=[{Node2,Ctr2}=NCT2|NClock], AccClock) ->
    if Node1 < Node2 ->
            merge(VClock, N, [NCT1|AccClock]);
       Node1 > Node2 ->
            merge(V, NClock, [NCT2|AccClock]);
       true ->
            CT = if Ctr1 > Ctr2 -> Ctr1;
                                   Ctr1 < Ctr2 -> Ctr2;
                                   true        -> Ctr1
                                end,
            merge(VClock, NClock, [{Node1,CT}|AccClock])
    end.

% @doc Get the counter value in VClock set from Node.
-spec get_counter(Node :: vclock_node(), VClock :: vclock()) -> counter().
get_counter(Node, VClock) ->
    case lists:keyfind(Node, 1, VClock) of
	{_, Ctr} -> Ctr;
	false           -> 0
    end.

% @doc Increment VClock at Node.
-spec increment(Node :: vclock_node(),
                VClock :: vclock()) -> vclock().
increment(Node, VClock) ->
    {Ctr,NewV} = case lists:keytake(Node, 1, VClock) of
                                false ->
                                    {1, VClock};
                                {value, {_N, C}, ModV} ->
                                    {C + 1, ModV}
                            end,
    [{Node,Ctr}|NewV].


% @doc Return the list of all nodes that have ever incremented VClock.
-spec all_nodes(VClock :: vclock()) -> [vclock_node()].
all_nodes(VClock) ->
    [X || {X, _} <- sort(VClock)].

% @doc Compares two VClocks for equality.
-spec equal(VClockA :: vclock(), VClockB :: vclock()) -> boolean().
equal(VA,VB) ->
    lists:sort(VA) =:= lists:sort(VB).

%% @doc sorts the vclock by actor
-spec sort(vclock()) -> vclock().
sort(Clock) ->
    lists:sort(Clock).

%% @doc an effecient format for disk / wire.
%5 @see `from_binary/1`
-spec to_binary(vclock()) -> binary_vclock().
to_binary(Clock) ->
    term_to_binary(sort(Clock)).

%% @doc takes the output of `to_binary/1` and returns a vclock
-spec from_binary(binary_vclock()) -> vclock().
from_binary(Bin) ->
    sort(binary_to_term(Bin)).

%% @doc replace actors in the vclock with those in the provided
%% 2 tuple list, the first element in the
%% actor map is present in the clock, and the second will
%% replace it.
-spec replace_actors([{vclock_node(), vclock_node()}], vclock()) -> vclock().
replace_actors(ActorMap, Vclock) ->
    replace_actors(ActorMap, Vclock, 1).

%% @doc replace the actors in the vclock with the actors
%% in the provided actor map list.
%% The element at `KeyPos` is the actor in the vclock
%% the other element in the actor map 2 tuple replaces it.
-spec replace_actors([{vclock_node(), vclock_node()}], vclock(), 1 | 2) -> vclock().
replace_actors(ActorMap, Vclock, KeyPos) ->
    lists:foldl(fun({CurrActor, Cntr}, NewClock) ->
                        Map = lists:keyfind(CurrActor, KeyPos, ActorMap),
                        [{new_key(Map, KeyPos), Cntr} | NewClock] end,
                [],
                Vclock).

new_key({_Old, New}, 1) ->
    New;
new_key({New, _Old}, 2) ->
    New.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

% doc Serves as both a trivial test and some example code.
example_test() ->
    A = riak_dt_vclock:fresh(),
    B = riak_dt_vclock:fresh(),
    A1 = riak_dt_vclock:increment(a, A),
    B1 = riak_dt_vclock:increment(b, B),
    true = riak_dt_vclock:descends(A1,A),
    true = riak_dt_vclock:descends(B1,B),
    false = riak_dt_vclock:descends(A1,B1),
    A2 = riak_dt_vclock:increment(a, A1),
    C = riak_dt_vclock:merge([A2, B1]),
    C1 = riak_dt_vclock:increment(c, C),
    true = riak_dt_vclock:descends(C1, A2),
    true = riak_dt_vclock:descends(C1, B1),
    false = riak_dt_vclock:descends(B1, C1),
    false = riak_dt_vclock:descends(B1, A1),
    ok.

accessor_test() ->
    VC = [{<<"1">>,  1},
          {<<"2">>,  2}],
    ?assertEqual(1, get_counter(<<"1">>, VC)),
    ?assertEqual(2, get_counter(<<"2">>, VC)),
    ?assertEqual(0, get_counter(<<"3">>, VC)),
    ?assertEqual([<<"1">>, <<"2">>], all_nodes(VC)).

merge_test() ->
    VC1 = [{<<"1">>,  1},
           {<<"2">>,  2},
           {<<"4">>,  4}],
    VC2 = [{<<"3">>,  3},
           {<<"4">>,  3}],
    ?assertEqual([], merge(riak_dt_vclock:fresh())),
    ?assertEqual([{<<"1">>,1},{<<"2">>,2},{<<"3">>,3},{<<"4">>,4}],
                 merge([VC1, VC2])).

merge_less_left_test() ->
    VC1 = [{<<"5">>, 5}],
    VC2 = [{<<"6">>,  6}, {<<"7">>,  7}],
    ?assertEqual([{<<"5">>, 5},{<<"6">>, 6}, {<<"7">>, 7}],
                 riak_dt_vclock:merge([VC1, VC2])).

merge_less_right_test() ->
    VC1 = [{<<"6">>, 6}, {<<"7">>,  7}],
    VC2 = [{<<"5">>, 5}],
    ?assertEqual([{<<"5">>, 5},{<<"6">>,  6}, {<<"7">>,  7}],
                 riak_dt_vclock:merge([VC1, VC2])).

merge_same_id_test() ->
    VC1 = [{<<"1">>, 1},{<<"2">>,1}],
    VC2 = [{<<"1">>, 1},{<<"3">>,1}],
    ?assertEqual([{<<"1">>, 1},{<<"2">>,1},{<<"3">>,1}],
                 riak_dt_vclock:merge([VC1, VC2])).

-endif.
