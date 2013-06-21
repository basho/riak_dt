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
-export([new/0, value/1, update/3, merge/2, equal/2, to_binary/1, from_binary/1]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0]).
-endif.

-export_type([vvorset/0, vvorset_op/0]).

-opaque vvorset() :: {actorlist(), entries()}.

-type vvorset_op() :: {add, member()} | {remove, member()}.

%% a dict of actor() -> Alias::integer() mappings
%% Reason being to keep the vector clocks as small as
%% possible and not repeat actor names over and over.
-type actorlist() :: orddict:orddict().

-type actor() :: term().

%% a dict of member() -> member_info() mappings.
%% The vclock is simply a more effecient way of storing
%% knowledge about adds / removes than a UUID per add.
-type entries() :: orddict:orddict().

-type member_info() :: {boolean(), vclock:vclock()}.
-type member() :: term().

-spec new() -> vvorset().
new() ->
    %% Use a dict of actors, and represent the actors as ints
    %% a sort of compression
    {orddict:new(), orddict:new()}.

-spec value(vvorset()) -> [member()].
value({_Actors, Entries}) ->
    [K || {K, {Active, _Vclock}} <- orddict:to_list(Entries), Active == 1].

-spec update(vvorset_op(), actor(), vvorset()) -> vvorset().
update({add, Elem}, Actor, ORSet) ->
    add_elem(Actor, ORSet, Elem);
update({remove, Elem}, _Actor, {_Actors, Entries}=ORSet) ->
    remove_elem(orddict:find(Elem, Entries), Elem, ORSet).

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

-spec vclock_merge(vclock:vclock(), vclock:vclock(),
                   actorlist(), actorlist(), actorlist()) -> vclock:vclock().
vclock_merge(V10, V20, Actors1, Actors2, MergedActors) ->
    V1 = vclock:replace_actors(orddict:to_list(Actors1), V10, 2),
    V2 = vclock:replace_actors(orddict:to_list(Actors2), V20, 2),
    Merged = vclock:merge([V1, V2]),
    vclock:replace_actors(orddict:to_list(MergedActors), Merged).

%% @Doc determine if the entry is active or removed.
%% First argument is a remove vclock, second is an active vclock
-spec is_active_or_removed(vclock:vclock(), vclock:vclock(),
                           actorlist(), actorlist(), actorlist()) -> member_info().
is_active_or_removed(RemoveClock0, AddClock0, RemActors, AddActors, MergedActors) ->
    RemoveClock = vclock:replace_actors(orddict:to_list(RemActors), RemoveClock0, 2),
    AddClock = vclock:replace_actors(orddict:to_list(AddActors), AddClock0, 2),
    case (not vclock:descends(RemoveClock, AddClock)) of
        true ->
            {1, vclock:replace_actors(orddict:to_list(MergedActors), AddClock)};
        false ->
            {0, vclock:replace_actors(orddict:to_list(MergedActors), RemoveClock)}
    end.

-spec equal(vvorset(), vvorset()) -> boolean().
equal(ORSet1, ORSet2) ->
    ORSet1 == ORSet2.

%% Private
-spec add_elem(actor(), vvorset(), member()) -> vvorset().
add_elem(Actor, {Actors0, Entries}, Elem) ->
    {Placeholder, Actors} = actor_placeholder(Actor, Actors0),
    InitialValue = {1, vclock:increment(Placeholder, vclock:fresh())},
    {Actors, orddict:update(Elem, update_fun(Placeholder), InitialValue, Entries)}.

-spec update_fun(actor()) -> function().
update_fun(Actor) ->
    fun({_, Vclock}) ->
            {1, vclock:increment(Actor, Vclock)}
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

-spec remove_elem({ok, member_info()} | error, member(), vvorset()) -> vvorset().
remove_elem({ok, {1, Vclock}}, Elem, {AL, Dict}) ->
    {AL, orddict:store(Elem, {0, Vclock}, Dict)};
remove_elem({ok, {0, _Vclock}}, _Elem, ORSet) ->
    ORSet;
remove_elem(_, _Elem, ORSet) ->
    %% What @TODO?
    %% Throw an error? (seems best)
    ORSet.

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
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(init_state(), 1000, ?MODULE))]}.

%% EQC generator
gen_op() ->
    ?LET({Add, Remove}, gen_elems(),
         oneof([{add, Add}, {remove, Remove}])).

gen_elems() ->
    ?LET(A, int(), {A, oneof([A, int()])}).

init_state() ->
    {0, dict:new()}.

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
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)}.

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
