%% -------------------------------------------------------------------
%%
%% riak_dt_vvorset: Another convergent, replicated, state based observe remove set
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

%% @doc An OR-Set CRDT. An OR-Set allows the adding, and removal, of
%% elements. Should an add and remove be concurrent, the add
%% wins. This is acheived by keeping a tombstone set for removed
%% elements. In the literature the OR-Set is modelled as two sets: an
%% add set and a remove set for tombstones. When an element is to be
%% added to the set a unique identifier is created and what is added
%% to the set is an {id, element} pair. When a member is removed from
%% the set all present {id, element} pairs in the add set for element
%% are copied to the remove set. This is the "Observed Remove
%% (OR-Set)". If the element is concurrently added elsewhere the new,
%% unique {id, member} pair will not be in the remove set thus the add
%% wins.
%%
%% This implementation is a more space efficient version of that
%% model. We use a vector clock and simply increment the clock for the
%% actor when an element is re-added. We flag a clock as removed
%% (rather than copying) when an element is removed, and when we
%% merge, if the RHS clock for an element is not flagged removed &&
%% the clock dominates the LHS clock, the add was concurrent and
%% element is flagged a member of the set again. This allows the set
%% to stay stable in space requirements in the face of many, many
%% re-adds. We designed the set this way as we plan to use it as the
%% Key set for a Map CRDT that allows in place updating of the
%% co-domain CRDTs, and each such update must be treated as an add.
%%
%% @TODO The tombstones can only be removed with consensus, some kind
%% of garabge collection. This is yet to be added.
%%
%% @see riak_dt_multi, vclock
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek
%% Zawirski (2011) A comprehensive study of Convergent and Commutative
%% Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @end
-module(riak_dt_vvvorset).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2]).
-export([update/3, merge/2, equal/2, reset/2]).
-export([to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-export_type([vvorset/0, vvorset_op/0, binary_vvorset/0]).

-opaque vvorset() :: {riak_dt_vclock:vclock(), actorlist(), entries()}.
-opaque binary_vvorset() :: binary(). %% A binary that from_binary/1 will operate on.

-type vvorset_op() :: {add, member()} | {remove, member()} |
                      {add_all, [member()]} | {remove_all, [member()]} |
                      {update, [vvorset_op()]}.
-type vvorset_q()  :: size | {contains, term()} | tombstones.

%% a dict of actor() -> Alias::integer() mappings
%% Reason being to keep the vector clocks as small as
%% possible and not repeat actor names over and over.
-type actorlist() :: [ {actor(), integer()} ].
-type actor() :: riak_dt:actor().

%% a dict of member() -> member_info() mappings.
%% The vclock is simply a more effecient way of storing
%% knowledge about adds / removes than a UUID per add.
-type entries() :: [{member(), member_info()}].

-type member_info() :: riak_dt_vclock:vclock().
-type member() :: term().

-spec new() -> vvorset().
new() ->
    %% Use a dict of actors, and represent the actors as ints
    %% a sort of compression
    {riak_dt_vclock:fresh(), orddict:new(), orddict:new()}.

-spec value(vvorset()) -> [member()].
value({_Clock, _Actors, Entries}) ->
    [K || {K, _Vclock} <- orddict:to_list(Entries)].

-spec value(vvorset_q(), vvorset()) -> term().
value(size, ORset) ->
    length(value(ORset));
value({contains, Elem}, ORset) ->
    lists:member(Elem, value(ORset)).

-spec update(vvorset_op(), actor(), vvorset()) -> {ok, vvorset()} |
                                                  {error, {precondition ,{not_present, member()}}}.
%% @Doc take a list of Set operations and apply them to the set.
%% NOTE: either _all_ are applied, or _none_ are.
update({update, Ops}, Actor, ORSet) ->
    apply_ops(Ops, Actor, ORSet);
update({add, Elem}, Actor, ORSet) ->
    {ok, add_elem(Actor, ORSet, Elem)};
update({remove, Elem}, _Actor, ORSet) ->
    {_Clock, _Actors, Entries} = ORSet,
    remove_elem(orddict:find(Elem, Entries), Elem, ORSet);
update({add_all, Elems}, Actor, ORSet) ->
    ORSet2 = lists:foldl(fun(E, S) ->
                                 add_elem(Actor, S, E) end,
                         ORSet,
                         Elems),
    {ok, ORSet2};
%% @Doc note: this is atomic, either _all_ `Elems` are removed, or
%% none are.
update({remove_all, Elems}, Actor, ORSet) ->
    remove_all(Elems, Actor, ORSet).

apply_ops([], _Actor, ORSet) ->
    {ok, ORSet};
apply_ops([Op | Rest], Actor, ORSet) ->
    case update(Op, Actor, ORSet) of
        {ok, ORSet2} ->
            apply_ops(Rest, Actor, ORSet2);
        Error ->
            Error
    end.

remove_all([], _Actor, ORSet) ->
    {ok, ORSet};
remove_all([Elem | Rest], Actor, ORSet) ->
    case update({remove, Elem}, Actor, ORSet) of
        {ok, ORSet2} ->
            remove_all(Rest, Actor, ORSet2);
        Error ->
            Error
    end.

-spec merge(vvorset(), vvorset()) -> vvorset().
merge({Clock1, Actors1, Entries1}, {Clock2, Actors2, Entries2}) ->
    Actors = merge_actors(Actors1, Actors2),
    Clock = vclock_merge(Clock1, Clock2, Actors1, Actors2, Actors),

    SetClock1 = riak_dt_vclock:replace_actors(orddict:to_list(Actors1), Clock1, 2),
    SetClock2 = riak_dt_vclock:replace_actors(orddict:to_list(Actors2), Clock2, 2),
    %% If an element is in both dicts, merge it
    %% If it occurs in one, then see if it's clock is dominated by the others whole set clock
    %% If so, then drop it, if not, keep it
    Keys1 = sets:from_list(orddict:fetch_keys(Entries1)),
    Keys2 = sets:from_list(orddict:fetch_keys(Entries2)),
    CommonKeys = sets:intersection(Keys1, Keys2),
    Unique1 = sets:subtract(Keys1, CommonKeys),
    Unique2 = sets:subtract(Keys2, CommonKeys),
    %% All common keys need their clocks merging
    Entries00 = sets:fold(fun(Key, Acc) ->
                                  V1 = orddict:fetch(Key, Entries1),
                                  V2 = orddict:fetch(Key, Entries2),
                                  V = vclock_merge(V1, V2, Actors1, Actors2, Actors),
                                  orddict:store(Key, V, Acc) end,
                          orddict:new(),
                          CommonKeys),
    %% Consider all LHS's unique keys
    Entries0 = sets:fold(fun(Key, Acc) ->
                                 LHSClock = orddict:fetch(Key, Entries1),
                                 LHSClock1 = riak_dt_vclock:replace_actors(orddict:to_list(Actors1), LHSClock, 2),
                                 case (not riak_dt_vclock:descends(SetClock2, LHSClock1)) of
                                     true ->
                                         NewClock =  riak_dt_vclock:replace_actors(orddict:to_list(Actors), LHSClock1),
                                         orddict:store(Key, NewClock, Acc);
                                     false ->
                                         Acc
                                 end
                         end,
                         Entries00,
                         Unique1),

    %% Consider all RHS's unique keys
    Entries = sets:fold(fun(Key, Acc) ->
                                RHSClock = orddict:fetch(Key, Entries2),
                                RHSClock1 = riak_dt_vclock:replace_actors(orddict:to_list(Actors2), RHSClock, 2),
                                case (not riak_dt_vclock:descends(SetClock1, RHSClock1)) of
                                    true ->
                                        NewClock =  riak_dt_vclock:replace_actors(orddict:to_list(Actors), RHSClock1),
                                        orddict:store(Key, NewClock, Acc);
                                    false ->
                                        Acc
                                end
                        end,
                        Entries0,
                        Unique2),


    {Clock, Actors, Entries}.

-spec merge_actors(actorlist(), actorlist()) -> actorlist().
merge_actors(Actors1, Actors2) ->
    AL = [Actor || Actor <- orddict:fetch_keys(Actors1) ++ orddict:fetch_keys(Actors2)],
    Sorted = lists:usort(AL),
    {_Cnt, NewActors} = lists:foldl(fun(E, {Cnt, Dict}) ->
                                            {Cnt+1, orddict:store(E, Cnt, Dict)} end,
                                    {1, orddict:new()},
                                    Sorted),
    NewActors.

-spec vclock_merge(riak_dt_vclock:vclock(), riak_dt_vclock:vclock(),
                   actorlist(), actorlist(), actorlist()) -> riak_dt_vclock:vclock().
vclock_merge(V10, V20, Actors1, Actors2, MergedActors) ->
    V1 = riak_dt_vclock:replace_actors(orddict:to_list(Actors1), V10, 2),
    V2 = riak_dt_vclock:replace_actors(orddict:to_list(Actors2), V20, 2),
    Merged = riak_dt_vclock:merge([V1, V2]),
    riak_dt_vclock:replace_actors(orddict:to_list(MergedActors), Merged).

%% @Doc determine if the entry is active or removed.
%% First argument is a remove vclock, second is an active vclock
%% is_active_or_removed(RemoveClock0, AddClock0, RemActors, AddActors, MergedActors) ->
%%     RemoveClock = riak_dt_vclock:replace_actors(orddict:to_list(RemActors), RemoveClock0, 2),
%%     AddClock = riak_dt_vclock:replace_actors(orddict:to_list(AddActors), AddClock0, 2),
%%     case (not riak_dt_vclock:descends(RemoveClock, AddClock)) of
%%         true ->
%%             {1, riak_dt_vclock:replace_actors(orddict:to_list(MergedActors), AddClock)};
%%         false ->
%%             {0, riak_dt_vclock:replace_actors(orddict:to_list(MergedActors), RemoveClock)}
%%     end.

-spec equal(vvorset(), vvorset()) -> boolean().
equal(ORSet1, ORSet2) ->
    ORSet1 == ORSet2.

%% @Doc reset the set to empty, essentially have `Actor' remove all present members.
-spec reset(vvorset(), actor()) -> vvorset().
reset(Set, Actor) ->
    Members = value(Set),
    reset(Members, Actor, Set).

reset([], _Actor, Set) ->
    Set;
reset([Member | Rest], Actor, Set) ->
    {ok, Set2} = update({remove, Member}, Actor, Set),
    reset(Rest, Actor, Set2).

%% Private
-spec add_elem(actor(), vvorset(), member()) -> vvorset().
add_elem(Actor, {Clock, Actors0, Entries}, Elem) ->
    {Placeholder, Actors} = actor_placeholder(Actor, Actors0),
    NewClock = riak_dt_vclock:increment(Placeholder, Clock),
    {NewClock, Actors, orddict:store(Elem, NewClock, Entries)}.

-spec actor_placeholder(actor(), actorlist()) -> {non_neg_integer(), actorlist()}.
actor_placeholder(Actor, Actors) ->
    case orddict:find(Actor, Actors) of
        {ok, Placeholder} ->
            {Placeholder, Actors};
        error ->
            Placeholder = orddict:size(Actors) +1,
            {Placeholder, orddict:store(Actor, Placeholder, Actors)}
    end.

remove_elem({ok, _Vclock}, Elem, {Clock, AL, Dict}) ->
    %% TODO Should I increment the set wide vclock here???
    {ok, {Clock, AL, orddict:erase(Elem, Dict)}};
remove_elem(_, Elem, _ORSet) ->
    %% What @TODO?
    %% Throw an error? (seems best)
    {error, {precondition, {not_present, Elem}}}.

-define(TAG, 75).
-define(V1_VERS, 1).

-spec to_binary(vvorset()) -> binary().
to_binary(ORSet) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(ORSet))/binary>>.

-spec from_binary(binary()) -> vvorset().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

generate() ->
    ?LET(Members, list(int()),
         lists:foldl(fun(M, Set) ->
                            riak_dt_vvvorset:update({add, M}, choose(1, 50), Set) end,
                    riak_dt_vvorset:new(),
                    Members)).

%% EQC generator
gen_op() ->
    oneof([gen_updates(), gen_update()]).

gen_updates() ->
     {update, non_empty(list(gen_update()))}.

gen_update() ->
    oneof([{add, int()}, {remove, int()},
           {add_all, list(int())},
           {remove_all, list(int())}]).

init_state() ->
    {0, dict:new()}.

do_updates(_ID, [], _OldState, NewState) ->
    NewState;
do_updates(ID, [{_Action, []} | Rest], OldState, NewState) ->
    do_updates(ID, Rest, OldState, NewState);
do_updates(ID, [Update | Rest], OldState, NewState) ->
    case {Update, update_expected(ID, Update, NewState)} of
        {{Op, Arg}, NewState} when Op == remove;
                                   Op == remove_all ->
            %% precondition fail, or idempotent remove?
            {_Cnt, Dict} = NewState,
            {_A, R} = dict:fetch(ID, Dict),
            Removed = [ E || {E, _X} <- sets:to_list(R)],
            case member(Arg, Removed) of
                true ->
                    OldState;
                false ->
                    OldState
            end;
        {_, NewNewState} ->
            do_updates(ID, Rest, OldState, NewNewState)
    end.

member(_Arg, []) ->
    false;
member(Arg, L) when is_list(Arg) ->
    sets:is_subset(sets:from_list(Arg), sets:from_list(L));
member(Arg, L) ->
    lists:member(Arg, L).

update_expected(ID, {update, Updates}, State) ->
    do_updates(ID, lists:sort(Updates), State, State);
update_expected(ID, {add, Elem}, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)};
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)};
update_expected(ID, {add_all, Elems}, State) ->
    lists:foldl(fun(Elem, S) ->
                       update_expected(ID, {add, Elem}, S) end,
               State,
               Elems);
update_expected(ID, {remove_all, Elems}, {_Cnt, Dict}=State) ->
    %% Only if _all_ elements are in the set do we remove any elems
    {A, R} = dict:fetch(ID, Dict),
    Members = [E ||  {E, _X} <- sets:to_list(sets:union(A,R))],
    case sets:is_subset(sets:from_list(Elems), sets:from_list(Members)) of
        true ->
            lists:foldl(fun(Elem, S) ->
                                update_expected(ID, {remove, Elem}, S) end,
                        State,
                        Elems);
        false ->
            State
    end.


eqc_state_value({_Cnt, Dict}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Values = [ Elem || {Elem, _X} <- sets:to_list(Remaining)],
    lists:usort(Values).

-endif.

-endif.
