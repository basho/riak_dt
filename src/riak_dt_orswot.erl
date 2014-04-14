%% -------------------------------------------------------------------
%%
%% riak_dt_orswot: Tombstone-less, replicated, state based observe remove set
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
%% elements. Should an add and remove be concurrent, the add wins. In
%% this implementation there is a version vector for the whole set.
%% When an element is added to the set, the version vector is
%% incremented and the `{actor(), count()}' pair for that increment is
%% stored against the element as its "birth dot". Every time the
%% element is re-added to the set, its "birth dot" is updated to that
%% of the `{actor(), count()}' version vector entry resulting from the
%% add. When an element is removed, we simply drop it, no tombstones.
%%
%% When an element exists in replica A and not replica B, is it
%% because A added it and B has not yet seen that, or that B removed
%% it and A has not yet seen that? Usually the presence of a tombstone
%% arbitrates. In this implementation we compare the "birth dot" of
%% the present element to the clock in the Set it is absent from. If
%% the element dot is not "seen" by the Set clock, that means the
%% other set has yet to see this add, and the item is in the merged
%% Set. If the Set clock dominates the dot, that means the other Set
%% has removed this element already, and the item is not in the merged
%% Set.
%%
%% Essentially we've made a dotted version vector.
%%
%% @see riak_dt_multi, riak_dt_vclock
%%
%% @reference Marc Shapiro, Nuno Preguiça, Carlos Baquero, Marek
%% Zawirski (2011) A comprehensive study of Convergent and Commutative
%% Replicated Data Types. http://hal.upmc.fr/inria-00555588/
%%
%% @reference Annette Bieniusa, Marek Zawirski, Nuno Preguiça, Marc
%% Shapiro, Carlos Baquero, Valter Balegas, Sérgio Duarte (2012) An
%% Optimized Conﬂict-free Replicated Set
%% http://arxiv.org/abs/1210.3368
%%
%% @reference Nuno Preguiça, Carlos Baquero, Paulo Sérgio Almeida,
%% Victor Fonte, Ricardo Gonçalves http://arxiv.org/abs/1011.5808
%%
%% @end
-module(riak_dt_orswot).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
-define(NUMTESTS, 1000).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2]).
-export([update/3, update/4, merge/2, equal/2]).
-export([to_binary/1, from_binary/1]).
-export([precondition_context/1, stats/1, stat/2]).
-export([parent_clock/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
-export([init_state/0, generate/0, size/1]).

-endif.

-export_type([orswot/0, orswot_op/0, binary_orswot/0]).

-opaque orswot() :: {riak_dt_vclock:vclock(), entries(), deferred()}.
%% Only removes can be deferred, so a list of members to be removed
%% per context.
-type deferred() :: [{riak_dt_vclock:vclock(), [member()]}].
-type binary_orswot() :: binary(). %% A binary that from_binary/1 will operate on.

-type orswot_op() ::  {add, member()} | {remove, member()} |
                      {add_all, [member()]} | {remove_all, [member()]} |
                      {update, [orswot_op()]}.
-type orswot_q()  :: size | {contains, term()}.

-type actor() :: riak_dt:actor().

%% a dict of member() -> minimal_clock() mappings.  The
%% `minimal_clock()' is a more effecient way of storing knowledge
%% about adds / removes than a UUID per add.
-type entries() :: [{member(), minimal_clock()}].

%% a minimal clock is just the dots for the element, each dot being an
%% actor and event counter for when the element was added.
-type minimal_clock() :: [dot()].
-type dot() :: riak_dt:dot().
-type member() :: term().

-type precondition_error() :: {error, {precondition ,{not_present, member()}}}.

-spec new() -> orswot().
new() ->
    {riak_dt_vclock:fresh(), orddict:new(), orddict:new()}.

%% @doc sets the clock in the Set to that `Clock'. Used by a
%% containing Map for sub-CRDTs
-spec parent_clock(riak_dt_vclock:vclock(), orswot()) -> orswot().
parent_clock(Clock, {_SetClock, Entries, Deferred}) ->
    {Clock, Entries, Deferred}.

-spec value(orswot()) -> [member()].
value({_Clock, Entries, _Deferred}) ->
    [K || {K, _Dots} <- orddict:to_list(Entries)].

-spec value(orswot_q(), orswot()) -> term().
value(size, ORset) ->
    length(value(ORset));
value({contains, Elem}, ORset) ->
    lists:member(Elem, value(ORset)).

%% @doc take a list of Set operations and apply them to the set.
%% NOTE: either _all_ are applied, or _none_ are.
-spec update(orswot_op(), actor() | dot(), orswot()) -> {ok, orswot()} |
                                                precondition_error().
update({update, Ops}, Actor, ORSet) ->
    apply_ops(lists:sort(Ops), Actor, ORSet);
update({add, Elem}, Actor, ORSet) ->
    {ok, add_elem(Actor, ORSet, Elem)};
update({remove, Elem}, _Actor, ORSet) ->
    {_Clock, Entries, _Deferred} = ORSet,
    remove_elem(orddict:find(Elem, Entries), Elem, ORSet);
update({add_all, Elems}, Actor, ORSet) ->
    ORSet2 = lists:foldl(fun(E, S) ->
                                 add_elem(Actor, S, E) end,
                         ORSet,
                         Elems),
    {ok, ORSet2};
%% @doc note: this is atomic, either _all_ `Elems` are removed, or
%% none are.
update({remove_all, Elems}, Actor, ORSet) ->
    remove_all(Elems, Actor, ORSet).

-spec update(orswot_op(), actor() | dot(), orswot(), riak_dt:context()) ->
                    {ok, orswot()} | precondition_error().
update(Op, Actor, ORSet, undefined) ->
    update(Op, Actor, ORSet);
update({add, Elem}, Actor, ORSet, _Ctx) ->
    ORSet2 = add_elem(Actor, ORSet, Elem),
    {ok, ORSet2};
update({remove, Elem}, _Actor, {Clock, Entries, Deferred}, Ctx) ->
    %% Being asked to remove something with a context.  If we
    %% have this element, we can drop any dots it has that the
    %% Context has seen.
    Deferred2 = defer_remove(Clock, Ctx, Elem, Deferred),
    case orddict:find(Elem, Entries) of
        {ok, ElemClock} ->
            ElemClock2 = riak_dt_vclock:subtract_dots(ElemClock, Ctx),
            case ElemClock2 of
                [] ->
                    {ok, {Clock, orddict:erase(Elem, Entries), Deferred2}};
                _ ->
                    {ok, {Clock, orddict:store(Elem, ElemClock2, Entries), Deferred2}}
            end;
        error ->
            %% Do we not have the element because we removed it
            %% already, or because we haven't seen the add?  Should
            %% there be a precondition error if Clock descends Ctx?
            %% In a way it makes no sense to have a precon error here,
            %% as the precon has been satisfied or will be: this is
            %% either deferred or a NO-OP
            {ok, {Clock, Entries, Deferred2}}
    end;
update({update, Ops}, Actor, ORSet, Ctx) ->
    ORSet2 = lists:foldl(fun(Op, Set) ->
                                 {ok, NewSet} = update(Op, Actor, Set, Ctx),
                                 NewSet
                         end,
                         ORSet,
                         Ops),
    {ok, ORSet2};
update({remove_all, Elems}, Actor, ORSet, Ctx) ->
    remove_all(Elems, Actor, ORSet, Ctx);
update({add_all, Elems}, Actor, ORSet, _Ctx) ->
    update({add_all, Elems}, Actor, ORSet).


%% @private If we're asked to remove something we don't have (or have,
%% but maybe not having seen all 'adds' for the element), is it
%% because we've not seen the add that we've been asked to remove, or
%% is it because we already removed it? In the former case, we can
%% "defer" this operation by storing it, with its context, for later
%% execution. If the clock for the Set descends the operation clock,
%% then we don't need to defer the op, its already been done. It is
%% _very_ important to note, that only _actorless_ operations can be
%% saved. That is operations that DO NOT increment the clock. In
%% ORSWOTS this is easy, as only removes need a context. A context for
%% an 'add' is meaningless.
%%
%% @TODO revist this, as it might be meaningful in some cases (for
%% true idempotence) and we can have merges triggering updates,
%% maybe.)
-spec defer_remove(riak_dt_vclock:vclock(), riak_dt_vclock:vclock(), orswot_op(), deferred()) ->
                      deferred().
defer_remove(Clock, Ctx, Elem, Deferred) ->
    case riak_dt_vclock:descends(Clock, Ctx) of
        %% no need to save this remove, we're done
        true -> Deferred;
        false -> orddict:update(Ctx,
                                fun(Elems) ->
                                        ordsets:add_element(Elem, Elems) end,
                                ordsets:add_element(Elem, ordsets:new()),
                                Deferred)
    end.

-spec apply_ops([orswot_op], actor() | dot(), orswot()) ->
                       {ok, orswot()} | precondition_error().
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

remove_all([], _Actor, ORSet, _Ctx) ->
    {ok, ORSet};
remove_all([Elem | Rest], Actor, ORSet, Ctx) ->
    {ok, ORSet2} =  update({remove, Elem}, Actor, ORSet, Ctx),
    remove_all(Rest, Actor, ORSet2, Ctx).

-spec merge(orswot(), orswot()) -> orswot().
merge({Clock, Entries, Deferred}, {Clock, Entries, Deferred}) ->
    {Clock, Entries, Deferred};
merge({LHSClock, LHSEntries, LHSDeferred}=LHS, {RHSClock, RHSEntries, RHSDeferred}=RHS) ->
    Clock = riak_dt_vclock:merge([LHSClock, RHSClock]),
    %% If an element is in both dicts, merge it. If it occurs in one,
    %% then see if its dots are dominated by the others whole set
    %% clock. If so, then drop it, if not, keep it.
    LHSKeys = sets:from_list(orddict:fetch_keys(LHSEntries)),
    RHSKeys = sets:from_list(orddict:fetch_keys(RHSEntries)),
    CommonKeys = sets:intersection(LHSKeys, RHSKeys),
    LHSUnique = sets:subtract(LHSKeys, CommonKeys),
    RHSUnique = sets:subtract(RHSKeys, CommonKeys),
    Entries00 = merge_common_keys(CommonKeys, LHS, RHS),

    Entries0 = merge_disjoint_keys(LHSUnique, LHSEntries, RHSClock, Entries00),
    Entries = merge_disjoint_keys(RHSUnique, RHSEntries, LHSClock, Entries0),

    Deffered = merge_deferred(LHSDeferred, RHSDeferred),

    apply_deferred(Clock, Entries, Deffered).

%% @private merge the deffered operations for both sets.
-spec merge_deferred(deferred(), deferred()) -> deferred().
merge_deferred(LHS, RHS) ->
    orddict:merge(fun(_K, LH, RH) ->
                          ordsets:union(LH, RH) end,
                  LHS, RHS).

%% @private any operation in the deferred list that has been seen as a
%% result of the merge, can be applied
%%
%% @TODO again, think hard on this, should it be called in process by
%% an actor only?
-spec apply_deferred(riak_dt_vclock:vclock(), entries(), deferred()) ->
                            orswot().
apply_deferred(Clock, Entries, Deferred) ->
    lists:foldl(fun({Ctx, Elems}, ORSwot) ->
                        {ok, ORSwot2} = remove_all(Elems, undefined,  ORSwot, Ctx),
                        ORSwot2
                end,
                {Clock, Entries, []}, %% Start with an empty deferred list
                Deferred).

%% @doc check if each element in `Entries' should be in the merged
%% set.
-spec merge_disjoint_keys(set(), orddict:orddict(),
                          riak_dt_vclock:vclock(), orddict:orddict()) -> orddict:orddict().
merge_disjoint_keys(Keys, Entries, SetClock, Accumulator) ->
    sets:fold(fun(Key, Acc) ->
                      Dots = orddict:fetch(Key, Entries),
                      case riak_dt_vclock:descends(SetClock, Dots) of
                          false ->
                              %% Optimise the set of stored dots to
                              %% include only those unseen
                              NewDots = riak_dt_vclock:subtract_dots(Dots, SetClock),
                              orddict:store(Key, NewDots, Acc);
                          true ->
                              Acc
                      end
              end,
              Accumulator,
              Keys).

%% @doc merges the minimal clocks for the common entries in both sets.
-spec merge_common_keys(set(), {riak_dt_vclock:vclock(), entries(), deferred()},
                        {riak_dt_vclock:vclock(), entries(), deferred()}) ->
                               orddict:orddict().
merge_common_keys(CommonKeys, {LHSClock, LHSEntries, _}, {RHSClock, RHSEntries, _}) ->

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
                      LHSKeep = riak_dt_vclock:subtract_dots(LHSUnique, RHSClock),
                      RHSKeep = riak_dt_vclock:subtract_dots(RHSUnique, LHSClock),
                      V = riak_dt_vclock:merge([sets:to_list(CommonDots), LHSKeep, RHSKeep]),
                      %% Perfectly possible that an item in both sets should be dropped
                      case V of
                          [] ->
                              orddict:erase(Key, Acc);
                          _ ->
                              orddict:store(Key, V, Acc)
                      end
              end,
              orddict:new(),
              CommonKeys).

-spec equal(orswot(), orswot()) -> boolean().
equal({Clock1, Entries1, _}, {Clock2, Entries2, _}) ->
    riak_dt_vclock:equal(Clock1, Clock2) andalso
        orddict:fetch_keys(Entries1) == orddict:fetch_keys(Entries2) andalso
        clocks_equal(Entries1, Entries2).

-spec clocks_equal(orddict:orddict(), orddict:orddict()) -> boolean().
clocks_equal([], _) ->
    true;
clocks_equal([{Elem, Clock1} | Rest], Entries2) ->
    Clock2 = orddict:fetch(Elem, Entries2),
    case riak_dt_vclock:equal(Clock1, Clock2) of
        true ->
            clocks_equal(Rest, Entries2);
        false ->
            false
    end.

%% Private
-spec add_elem(actor() | dot(), orswot(), member()) -> orswot().
add_elem(Dot, {Clock, Entries, Deferred}, Elem) when is_tuple(Dot) ->
    {riak_dt_vclock:merge([Clock, [Dot]]), update_entry(Elem, Entries, [Dot]), Deferred};
add_elem(Actor, {Clock, Entries, Deferred}, Elem) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Dot = [{Actor, riak_dt_vclock:get_counter(Actor, NewClock)}],
    {NewClock, update_entry(Elem, Entries, Dot), Deferred}.

-spec update_entry(member(), orddict:orddict(), riak_dt_vclock:vclock()) ->
                          orddict:orddict().
update_entry(Elem, Entries, Dot) ->
    orddict:update(Elem, fun(Clock) ->
                                 riak_dt_vclock:merge([Clock, Dot]) end,
                   Dot,
                   Entries).

-spec remove_elem({ok, riak_dt_vclock:vclock()} | error,
                  member(), {riak_dt_vclock:vclock(), orddict:orddict(), deferred()}) ->
                         {ok, {riak_dt_vclock:vclock(), orddict:orddict(), deferred()}} |
                         precondition_error().
remove_elem({ok, _VClock}, Elem, {Clock, Dict, Deferred}) ->
    {ok, {Clock, orddict:erase(Elem, Dict), Deferred}};
remove_elem(_, Elem, _ORSet) ->
    {error, {precondition, {not_present, Elem}}}.

%% @doc the precondition context is a fragment of the CRDT that
%%  operations requiring certain pre-conditions can be applied with.
%%  Especially useful for hybrid op/state systems where the context of
%%  an operation is needed at a replica without sending the entire
%%  state to the client. In the case of the ORSWOT the context is a
%%  version vector. When passed as an argument to `update/4' the
%%  context ensures that only seen adds are removed, and that removes
%%  of unseen adds can be deferred until they're seen.
-spec precondition_context(orswot()) -> orswot().
precondition_context({Clock, _Entries, _Deferred}) ->
    Clock.

-spec stats(orswot()) -> [{atom(), number()}].
stats(ORSWOT) ->
    [ {S, stat(S, ORSWOT)} || S <- [actor_count, element_count, max_dot_length, deferred_length]].

-spec stat(atom(), orswot()) -> number() | undefined.
stat(actor_count, {Clock, _Dict, _}) ->
    length(Clock);
stat(element_count, {_Clock, Dict, _}) ->
    orddict:size(Dict);
stat(max_dot_length, {_Clock, Dict, _}) ->
    orddict:fold(fun(_K, Dots, Acc) ->
                         max(length(Dots), Acc)
                 end, 0, Dict);
stat(deferred_length, {_Clock, _Dict, Deferred}) ->
    length(Deferred);
stat(_,_) -> undefined.

-include("riak_dt_tags.hrl").
-define(TAG, ?DT_ORSWOT_TAG).
-define(V1_VERS, 1).

%% @doc returns a binary representation of the provided
%% `orswot()'. The resulting binary is tagged and versioned for ease
%% of future upgrade. Calling `from_binary/1' with the result of this
%% function will return the original set. Use the application env var
%% `binary_compression' to turn t2b compression on (`true') and off
%% (`false')
%%
%% @see `from_binary/1'
-spec to_binary(orswot()) -> binary_orswot().
to_binary(S) ->
     <<?TAG:8/integer, ?V1_VERS:8/integer, (riak_dt:to_binary(S))/binary>>.

%% @doc When the argument is a `binary_orswot()' produced by
%% `to_binary/1' will return the original `orswot()'.
%%
%% @see `to_binary/1'
-spec from_binary(binary_orswot()) -> orswot().
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    riak_dt:from_binary(B).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

stat_test() ->
    Set = new(),
    {ok, Set1} = update({add, <<"foo">>}, 1, Set),
    {ok, Set2} = update({add, <<"foo">>}, 2, Set1),
    {ok, Set3} = update({add, <<"bar">>}, 3, Set2),
    {ok, Set4} = update({remove, <<"foo">>}, 1, Set3),
    ?assertEqual([{actor_count, 0}, {element_count, 0},
                  {max_dot_length, 0}, {deferred_length, 0}],
                 stats(Set)),
    ?assertEqual(3, stat(actor_count, Set4)),
    ?assertEqual(1, stat(element_count, Set4)),
    ?assertEqual(1, stat(max_dot_length, Set4)),
    ?assertEqual(undefined, stat(waste_pct, Set4)).

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
    {ok, A2} = riak_dt_orswot:update({remove, 'Z'}, a, A),
    %% replicate B to A, now A has B's 'Z'
    A3 = riak_dt_orswot:merge(A2, B),
    %% Remove B's 'Z'
    {ok, B2} = riak_dt_orswot:update({remove, 'Z'}, b, B),
    %% Replicate C to B, now B has A's old 'Z'
    B3 = riak_dt_orswot:merge(B2, C),
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

-ifdef(EQC).

bin_roundtrip_test_() ->
    crdt_statem_eqc:run_binary_rt(?MODULE, ?NUMTESTS).

eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, ?NUMTESTS).

size(Set) ->
    value(size, Set).

generate() ->
    %% Only generate add ops
    ?LET({Ops, Actors}, {non_empty(list({add, bitstring(20*8)})), non_empty(list(bitstring(16*8)))},
         lists:foldl(fun(Op, Set) ->
                             Actor = case length(Actors) of
                                         1 -> hd(Actors);
                                         _ -> lists:nth(crypto:rand_uniform(1, length(Actors)), Actors)
                                     end,
                             case riak_dt_orswot:update(Op, Actor, Set) of
                                 {ok, S} -> S;
                                 _ -> Set
                             end
                     end,
                     riak_dt_orswot:new(),
                     Ops)).

%% EQC generator
gen_op() ->
    gen_op(fun() -> int() end).

gen_op(Gen) ->
    oneof([gen_updates(Gen), gen_update(Gen)]).

gen_updates(Gen) ->
    {update, non_empty(list(gen_update(Gen)))}.

gen_update(Gen) ->
    oneof([{add, Gen()}, {remove, Gen()},
           {add_all, non_empty(list(Gen()))},
           {remove_all, non_empty(list(Gen()))}]).

init_state() ->
    {0, dict:new()}.

do_updates(_ID, [], _OldState, NewState) ->
    NewState;
do_updates(ID, [Update | Rest], OldState, NewState) ->
    case {Update, update_expected(ID, Update, NewState)} of
        {{Op, _Arg}, NewState} when Op == remove;
                                   Op == remove_all ->
            OldState;
        {_, NewNewState} ->
            do_updates(ID, Rest, OldState, NewNewState)
    end.

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
    %% DO NOT consider tombstones as "in" the set, orswot does not have idempotent remove
    In = sets:subtract(A, R),
    Members = [ Elem || {Elem, _X} <- sets:to_list(In)],
    case is_sub_bag(Elems, lists:usort(Members)) of
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

%% Bag1 and Bag2 are multisets if Bag1 is a sub-multset of Bag2 return
%% true, else false
is_sub_bag(Bag1, Bag2) when length(Bag1) > length(Bag2) ->
    false;
is_sub_bag(Bag1, Bag2) ->
    SubBag = lists:sort(Bag1),
    SuperBag = lists:sort(Bag2),
    is_sub_bag2(SubBag, SuperBag).

is_sub_bag2([], _SuperBag) ->
    true;
is_sub_bag2([Elem | Rest], SuperBag) ->
    case lists:delete(Elem, SuperBag) of
        SuperBag ->
            false;
        SuperBag2 ->
            is_sub_bag2(Rest, SuperBag2)
    end.

-endif.

-endif.
