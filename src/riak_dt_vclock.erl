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
-compile(inline_list_funcs).

-export([fresh/0,descends/2,merge/1,get_counter/2, subtract_dots/2,
         increment/2,all_nodes/1, equal/2,
         to_binary/1, from_binary/1, dominates/2, glb/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% The presence of the special actor indicates the list is sorted
%% This is a very "low" actor # that was chosen randomly, and smaller than -2**31
-define(SPECIAL_DOT, {-34359738368, 1}).
-export_type([vclock/0, vclock_node/0, binary_vclock/0]).

-type vclock() :: [vc_entry()].
-type sorted_vclock() :: orddict:orddict(vclock_node(), counter()).
-type binary_vclock() :: binary().
% The timestamp is present but not used, in case a client wishes to inspect it.
-type vc_entry() :: {vclock_node(), counter()}.

% Nodes can have any term() as a name, but they must differ from each other.
-type   vclock_node() :: term().
-type   counter() :: non_neg_integer().

% @doc Create a brand new vclock.
-spec fresh() -> sorted_vclock().
fresh() ->
    [].

-spec(ensure_sorted(vclock()) -> sorted_vclock()).
ensure_sorted(VClock0) ->
    case lists:member(?SPECIAL_DOT, VClock0) of
        true ->
            VClock0;
        false ->
            lists:usort([?SPECIAL_DOT|VClock0])
    end.
%%    {SpecialActor, _} = ?SPECIAL_DOT,
%%    case lists:keyfind(SpecialActor, 1, VClock0) of
%%        {_, SpecialVersionCounter} when SpecialVersionCounter band 1 > 0 ->
%%            VClock0;
%%        Else ->
%%            ?debugFmt("Found: ~p~n", [Else]),
%%            After = lists:usort([?SPECIAL_DOT|VClock0]),
%%            ?debugFmt("After: ~p~n", [After]),
%%            After
%%
%%    end.


% @doc Return true if Va is a direct descendant of Vb, else false -- remember, a vclock is its own descendant!
-spec descends(Va :: vclock()|[], Vb :: vclock()|[]) -> boolean().
descends(Va0, Vb0) ->
    Va1 = ensure_sorted(Va0),
    Vb1 = ensure_sorted(Vb0),
    descends1(Va1, Vb1).


-spec descends1(Va :: sorted_vclock()|[], Vb :: sorted_vclock()|[]) -> boolean().
descends1(_, []) ->
    true;
descends1([{Actor, CounterA}|RestA], [{Actor, CounterB}|RestB]) when CounterA >= CounterB ->
    descends1(RestA, RestB);
descends1([{ActorA, _CounterA}|RestA], Vb = [{ActorB, _CounterB}|_RestB]) when ActorB >= ActorA ->
    descends1(RestA, Vb);
descends1(_, _) ->
    false.

% @doc Return true if Va strictly dominates Vb, else false!
-spec dominates(vclock(), vclock()) -> boolean().
dominates(Va0, Vb0) ->
    Va1 = ensure_sorted(Va0),
    Vb1 = ensure_sorted(Vb0),
    dominates1(Va1, Vb1, false).

-spec dominates1(sorted_vclock(), sorted_vclock(), boolean()) -> boolean().
dominates1(_, [], true) -> true;
dominates1([], [], HasDominated) -> HasDominated;
dominates1([{Actor, CounterA}|RestA], [{Actor, CounterB}|RestB], _HasDominated) when CounterA > CounterB ->
    dominates1(RestA, RestB, true);
dominates1([{Actor, Counter}|RestA], [{Actor, Counter}|RestB], HasDominated) ->
    dominates1(RestA, RestB, HasDominated);
dominates1([{Actor, CounterA}|_RestA], [{Actor, CounterB}|_RestB], _HasDominated) when CounterB > CounterA ->
    false;
dominates1([{ActorA, _CounterA}|RestA], Vb = [{ActorB, _CounterB}|_RestB], HasDominated) when ActorB >= ActorA ->
    dominates1(RestA, Vb, HasDominated);
dominates1(Va = [{ActorA, _CounterA}|_RestA], [{ActorB, _CounterB}|RestB], HasDominated) when ActorA >= ActorB ->
    dominates1(Va, RestB, HasDominated).

%% @doc subtract the VClock from the DotList.
%% what this means is that any {actor(), count()} pair in
%% DotList that is <= an entry in  VClock is removed from DotList
%% Example [{a, 3}, {b, 2}, {d, 14}, {g, 22}] -
%%         [{a, 4}, {b, 1}, {c, 1}, {d, 14}, {e, 5}, {f, 2}] =
%%         [{{b, 2}, {g, 22}]
-spec subtract_dots(vclock(), vclock()) -> vclock().
subtract_dots(DotList0, VClock0) ->
    DotList1 = ensure_sorted(DotList0),
    VClock1 = ensure_sorted(VClock0),
    drop_dots(DotList1, VClock1, []).

%% A - B
drop_dots([], _Clock, NewDots) ->
    lists:reverse(NewDots);
drop_dots(A, [], NewDots) ->
    lists:reverse(A) ++ lists:reverse(NewDots);
drop_dots([Dot = {Actor, CountA} | RestA], [{Actor, CountB} | RestB], Acc) when CountA > CountB ->
    drop_dots(RestA, RestB, [Dot|Acc]);
drop_dots([{Actor, CountA} | RestA], [{Actor, CountB} | RestB], Acc) when CountA =< CountB ->
    drop_dots(RestA, RestB, Acc);
drop_dots([Dot = {ActorA, _CountA} | RestA], B = [{ActorB, _CountB} | _RestB], Acc) when ActorB > ActorA ->
    drop_dots(RestA, B, [Dot|Acc]);
drop_dots(A = [{ActorA, _CountA} | _RestA], [{ActorB, _CountB} | RestB], Acc) when ActorA > ActorB ->
    drop_dots(A, RestB, Acc).



% @doc Combine all VClocks in the input list into their least possible
%      common descendant.
-spec merge(VClocks :: [vclock()]) -> vclock() | [].
merge(VClocks0) ->
    [VClocks1|RestVClocks1] = lists:map(fun ensure_sorted/1, VClocks0),
    lists:foldl(fun merge/2, VClocks1, RestVClocks1).

merge(V1, V2) ->
    merge(V1, V2, []).

merge([], [], Acc) ->
    lists:reverse(Acc);
merge([DotA = {Actor, CounterA}|RestVClockA], [_DotB = {Actor, CounterB}|RestVClockB], Acc) when CounterA >= CounterB ->
    merge(RestVClockA, RestVClockB, [DotA|Acc]);
merge([_DotA = {Actor, CounterA}|RestVClockA], [DotB = {Actor, CounterB}|RestVClockB], Acc) when CounterA < CounterB ->
    merge(RestVClockA, RestVClockB, [DotB|Acc]);
merge(VClockA = [{ActorA, _}|_RestVClockA], [DotB = {ActorB, _}|RestVClockB], Acc) when ActorA > ActorB->
    merge(VClockA, RestVClockB, [DotB|Acc]);
merge([DotA = {ActorA, _}|RestVClockA], VClockB = [{ActorB, _}|_RestVClockB], Acc) when ActorA < ActorB->
    merge(RestVClockA, VClockB, [DotA|Acc]);
merge([], VClockB, Acc) ->
    lists:reverse(Acc) ++ VClockB;
merge(VClockA, [], Acc) ->
    lists:reverse(Acc) ++ VClockA.




% @doc Get the counter value in VClock set from Node.
-spec get_counter(Node :: vclock_node(), VClock :: vclock()) -> counter().
get_counter(Node, VClock) ->
    %% No reason to try sorting it
    %% Best case scenario sort is O(N)
    %% This function's worst case scenario is O(N)
    case lists:keyfind(Node, 1, VClock) of
	{_, Ctr} -> Ctr;
	false           -> 0
    end.

% @doc Increment VClock at Node.
-spec increment(Node :: vclock_node(),
                VClock :: vclock()) -> vclock().
increment(Node, VClock0) ->
    VClock1 = ensure_sorted(VClock0),
    orddict:update(Node, fun(X) -> X + 1 end, 1, VClock1).

% @doc Return the list of all nodes that have ever incremented VClock.
-spec all_nodes(VClock :: vclock()) -> [vclock_node()].
all_nodes(VClock0) ->
    lists:usort([X || {X, _} <- VClock0]).

% @doc Compares two VClocks for equality.
-spec equal(VClockA :: vclock(), VClockB :: vclock()) -> boolean().
equal(VA,VB) ->
    ensure_sorted(VA) =:= ensure_sorted(VB).

%% @doc sorts the vclock by actor
-spec sort(vclock()) -> vclock().
sort(Clock) ->
    lists:usort(Clock).

%% @doc an effecient format for disk / wire.
%5 @see `from_binary/1`
-spec to_binary(vclock()) -> binary_vclock().
to_binary(Clock) ->
    term_to_binary(sort(Clock)).

%% @doc takes the output of `to_binary/1` and returns a vclock
-spec from_binary(binary_vclock()) -> vclock().
from_binary(Bin) ->
    sort(binary_to_term(Bin)).

%% @doc take two vclocks and return a vclock that summerizes only the
%% events both have seen.
-spec glb(vclock(), vclock()) -> sorted_vclock().
glb(ClockA0, ClockB0) ->
    ClockA1 = ensure_sorted(ClockA0),
    ClockB1 = ensure_sorted(ClockB0),
    glb(ClockA1, ClockB1, []).

glb([], [], Acc) ->
    lists:reverse(Acc);
glb(_, [], Acc) ->
    glb([], [], Acc);
glb([], _, Acc) ->
    glb([], [], Acc);
glb([Dot|RestClockA], [Dot|RestClockB], Acc) ->
    glb(RestClockA, RestClockB, Acc);
glb([{Actor, CounterA}|RestClockA], [DotB = {Actor, CounterB}|RestClockB], Acc) when CounterA > CounterB ->
    glb(RestClockA, RestClockB, [DotB|Acc]);
glb([DotA = {Actor, CounterA}|RestClockA], [{Actor, CounterB}|RestClockB], Acc) when CounterB > CounterA ->
    glb(RestClockA, RestClockB, [DotA|Acc]);
glb([{ActorA, _}|RestClockA], ClockB = [{ActorB, _}|_RestClockB], Acc) when ActorA < ActorB ->
    glb(RestClockA, ClockB, Acc);
glb(ClockA = [{ActorA, _}|_RestClockA], [{ActorB, _}|RestClockB], Acc) when ActorA > ActorB ->
    glb(ClockA, RestClockB, Acc).

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
    ?assertEqual([?SPECIAL_DOT], merge([riak_dt_vclock:fresh()])),
    ?assertEqual([?SPECIAL_DOT, {<<"1">>,1},{<<"2">>,2},{<<"3">>,3},{<<"4">>,4}],
                 merge([VC1, VC2])).

merge_less_left_test() ->
    VC1 = [{<<"5">>, 5}],
    VC2 = [{<<"6">>,  6}, {<<"7">>,  7}],
    ?assertEqual([?SPECIAL_DOT, {<<"5">>, 5},{<<"6">>, 6}, {<<"7">>, 7}],
                 riak_dt_vclock:merge([VC1, VC2])).

merge_less_right_test() ->
    VC1 = [{<<"6">>, 6}, {<<"7">>,  7}],
    VC2 = [{<<"5">>, 5}],
    ?assertEqual([?SPECIAL_DOT, {<<"5">>, 5},{<<"6">>,  6}, {<<"7">>,  7}],
                 riak_dt_vclock:merge([VC1, VC2])).

merge_same_id_test() ->
    VC1 = [{<<"1">>, 1},{<<"2">>,1}],
    VC2 = [{<<"1">>, 1},{<<"3">>,1}],
    ?assertEqual([?SPECIAL_DOT, {<<"1">>, 1},{<<"2">>,1},{<<"3">>,1}],
                 riak_dt_vclock:merge([VC1, VC2])).

random_clock1(N1, N2) ->
    Seq0 = lists:seq(N1, N2),
    Seq1 = [{rand:uniform(1000000), X} || X <- Seq0],
    Seq2 = lists:sort(Seq1),
    Seq3 = [X || {_, X} <- Seq2],
    lists:map(
        fun(I) ->
            Actor = list_to_atom(lists:flatten(io_lib:format("actor-~b", [I]))),
            {Actor, random:uniform(100000)}
        end,
        Seq3
    ).
get_time(Fun, Args) ->
    T1 = os:timestamp(),
    lists:foreach(fun(_) -> apply(Fun, Args) end, lists:seq(1, 10000)),
    T2 = os:timestamp(),
    DiffMicros = timer:now_diff(T2, T1),
    round(DiffMicros / 10000.0).

bench_test_() ->
    {timeout, 300, [fun() -> bench() end]}.
bench() ->
    A = random_clock1(1, 1000),
    A1 = ensure_sorted(A),
    B = random_clock1(1, 1000),
    B1 = ensure_sorted(B),
    C = random_clock1(500, 1500),
    C1 = ensure_sorted(C),
    D = random_clock1(1, 1500),
    D1 = ensure_sorted(D),
    E = random_clock1(1, 5),
    E1 = ensure_sorted(E),

    ?debugFmt("Increment Time (1): ~b~n", [get_time(fun increment/2, ['actor-500', A])]),
    ?debugFmt("Increment Time (2): ~b~n", [get_time(fun increment/2, ['actor-500', A1])]),
    ?debugFmt("Merge Time (1): ~b~n", [get_time(fun merge/1, [[B, A]])]),
    ?debugFmt("Merge Time (2): ~b~n", [get_time(fun merge/1, [[B1, A1]])]),
    ?debugFmt("Merge Time (3): ~b~n", [get_time(fun merge/1, [[A, C]])]),
    ?debugFmt("Merge Time (4): ~b~n", [get_time(fun merge/1, [[A1, C1]])]),
    ?debugFmt("Merge Time (5): ~b~n", [get_time(fun merge/1, [[A, A]])]),
    ?debugFmt("Merge Time (6): ~b~n", [get_time(fun merge/1, [[A1, A1]])]),
    ?debugFmt("Merge Time (7): ~b~n", [get_time(fun merge/1, [[E, E]])]),
    ?debugFmt("Merge Time (8): ~b~n", [get_time(fun merge/1, [[E1, E1]])]),

    ?debugFmt("Descends Time (1): ~b~n", [get_time(fun descends/2, [A, C])]),
    ?debugFmt("Descends Time (2): ~b~n", [get_time(fun descends/2, [A1, C1])]),
    ?debugFmt("Descends Time (2): ~b~n", [get_time(fun descends/2, [A, D])]),
    ?debugFmt("Descends Time (3): ~b~n", [get_time(fun descends/2, [A1, D1])]),
    ?debugFmt("Descends Time (4): ~b~n", [get_time(fun descends/2, [D, A])]),
    ?debugFmt("Descends Time (5): ~b~n", [get_time(fun descends/2, [D1, A1])]),
    ?debugFmt("Descends Time (6): ~b~n", [get_time(fun descends/2, [D, D])]),
    ?debugFmt("Descends Time (7): ~b~n", [get_time(fun descends/2, [D1, D1])]).

-endif.
