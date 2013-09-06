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
-module(riak_dt_vvorset).

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
-export([precondition_context/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0, generate/0]).
-endif.

-export_type([vvorset/0, vvorset_op/0, binary_vvorset/0]).

-opaque vvorset() :: {actorlist(), entries()}.
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

-type member_info() :: {boolean(), riak_dt_vclock:vclock()}.
-type member() :: term().

-spec new() -> vvorset().
new() ->
    %% Use a dict of actors, and represent the actors as ints
    %% a sort of compression
    {orddict:new(), orddict:new()}.

-spec value(vvorset()) -> [member()].
value({_Actors, Entries}) ->
    [K || {K, {Active, _Vclock}} <- orddict:to_list(Entries), Active == 1].

%% @doc Query `OR-Set`
-spec value(vvorset_q(), vvorset()) -> term().
value(size, ORset) ->
    length(value(ORset));
value({contains, Elem}, ORset) ->
    lists:member(Elem, value(ORset));
value(tombstones, {_Actors, Entries}) ->
    [K || {K, {Active, _Vclock}} <- orddict:to_list(Entries), Active == 0].

-spec update(vvorset_op(), actor(), vvorset()) -> {ok, vvorset()} |
                                                  {error, {precondition ,{not_present, member()}}}.
%% @Doc take a list of Set operations and apply them to the set.
%% NOTE: either _all_ are applied, or _none_ are.
update({update, Ops}, Actor, ORSet) ->
    %% Sort ops, so adds are before removes.
    apply_ops(lists:sort(Ops), Actor, ORSet);
update({add, Elem}, Actor, ORSet) ->
    {ok, add_elem(Actor, ORSet, Elem)};
update({remove, Elem}, _Actor, ORSet) ->
    {_Actors, Entries} = ORSet,
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
merge({Actors1, Entries1}, {Actors2, Entries2}) ->
    %% for every key in Entries1, merge its contents with Entries2's content for same key
    %% if the keys values are both active or both deleted, simply merge the clocks
    %% if one is active and one is deleted, check that the deleted dominates the added
    %% if so, set to that value and deleted, otherwise keep as is (no need to merge vclocks?)
    %% Either way we have to restructure _all_ the vclocks to reflect the merged actor ids
    Actors = merge_actors(Actors1, Actors2),
    {Actors, orddict:merge(fun(_K, {Bool, V1}, {Bool, V2}) ->
                                   {Bool, vclock_merge(V1, V2, Actors1, Actors2, Actors)};
                     (_K, {0, V1}, {1, V2}) ->
                                   is_active_or_removed(V1, V2, Actors1, Actors2, Actors);
                     (_K, {1, V1}, {0, V2}) ->
                                   is_active_or_removed(V2, V1, Actors2, Actors1, Actors) end,
                     Entries1, Entries2)}.

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
is_active_or_removed(RemoveClock0, AddClock0, RemActors, AddActors, MergedActors) ->
    RemoveClock = riak_dt_vclock:replace_actors(orddict:to_list(RemActors), RemoveClock0, 2),
    AddClock = riak_dt_vclock:replace_actors(orddict:to_list(AddActors), AddClock0, 2),
    case (not riak_dt_vclock:descends(RemoveClock, AddClock)) of
        true ->
            {1, riak_dt_vclock:replace_actors(orddict:to_list(MergedActors), AddClock)};
        false ->
            {0, riak_dt_vclock:replace_actors(orddict:to_list(MergedActors), RemoveClock)}
    end.

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
add_elem(Actor, {Actors0, Entries}, Elem) ->
    {Placeholder, Actors} = actor_placeholder(Actor, Actors0),
    InitialValue = {1, riak_dt_vclock:increment(Placeholder, riak_dt_vclock:fresh())},
    {Actors, orddict:update(Elem, update_fun(Placeholder), InitialValue, Entries)}.

-spec update_fun(actor()) -> function().
update_fun(Actor) ->
    fun({_, Vclock}) ->
            {1, riak_dt_vclock:increment(Actor, Vclock)}
    end.

-spec actor_placeholder(actor(), actorlist()) -> {non_neg_integer(), actorlist()}.
actor_placeholder(Actor, Actors) ->
    case orddict:find(Actor, Actors) of
        {ok, Placeholder} ->
            {Placeholder, Actors};
        error ->
            Placeholder = orddict:size(Actors) +1,
            {Placeholder, orddict:store(Actor, Placeholder, Actors)}
    end.

remove_elem({ok, {1, Vclock}}, Elem, {AL, Dict}) ->
    {ok, {AL, orddict:store(Elem, {0, Vclock}, Dict)}};
remove_elem({ok, {0, _Vclock}}, _Elem, ORSet) ->
    {ok, ORSet};
remove_elem(_, Elem, _ORSet) ->
    %% What @TODO?
    %% Throw an error? (seems best)
    {error, {precondition, {not_present, Elem}}}.

%% @doc the precondition context is a binary representation of a fragment of the CRDT
%% that operations with pre-conditions can be applied too.
%% In the case of OR-Sets this is the set of adds observed.
%% The system can then apply a remove to this context and merge it with a replica.
%% Especially useful for hybrid op/state systems where the context of an operation is
%% needed at a replica without sending the entire state to the client.
-spec precondition_context(vvorset()) -> vvorset().
precondition_context({AL, Entries}) ->
    {AL, [Add || {_K, {Active, _Clock}}=Add <- orddict:to_list(Entries),
                 Active == 1]}.

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
                            riak_dt_vvorset:update({add, M}, choose(1, 50), Set) end,
                    riak_dt_vvorset:new(),
                    Members)).

%% EQC generator
gen_op() ->
   riak_dt_orset:gen_op().

init_state() ->
    riak_dt_orset:init_state().

update_expected(ID, Op, State) ->
    riak_dt_orset:update_expected(ID, Op, State).

eqc_state_value(State) ->
    riak_dt_orset:eqc_state_value(State).

-endif.

query_test() ->
    Set = new(),
    {ok, Set2} = update({add, bob}, a1, Set),
    {ok, Set3} = update({add, pete}, a2, Set2),
    {ok, Set4} = update({add, sheila}, a3, Set3),
    {ok, Set5} = update({remove, pete}, a3, Set4),
    {ok, Set6} = update({remove, bob}, a2, Set5),
    {ok, Set7} = update({add, dave}, a1, Set6),
    ?assertEqual(2, value(size, Set7)),
    ?assert(value({contains, sheila}, Set7)),
    ?assertNot(value({contains, bob}, Set7)),
    ?assertEqual([bob, pete], value(tombstones, Set7)).

-endif.
