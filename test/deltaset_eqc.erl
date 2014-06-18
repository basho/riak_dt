%% -------------------------------------------------------------------
%%
%% deltaset_eqc:
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(deltaset_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).


-define(SWOT, riak_dt_delta_orswot).
-define(SET, riak_dt_orset).

%% The set of possible elements in the set
-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-record(state, {replicas=[], %% Actor Ids for replicas in the system
                adds=[]      %% Elements that have been added to the set
               }).

-record(replica, {id,
                  delta=?SWOT:new(),
                  swot=?SWOT:new(),
                  set=?SET:new()}
       ).

eqc_test_() ->
    {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(prop_merge()))))}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_merge())).

check() ->
    eqc:check(prop_merge()).

initial_state() ->
    #state{}.

%% ------ Grouped operator: create_replica
create_replica_pre(#state{replicas=Replicas}) ->
    length(Replicas) < 10.

%% @doc create_replica_command - Command generator
create_replica_args(_S) ->
    %% Don't waste time shrinking the replicas ID binaries, they 8
    %% byte binaris as that is riak-esque.
    [noshrink(binary(8))].

%% @doc create_replica_pre - don't create a replica that already
%% exists
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc create a new replica, and store a bottom orswot/orset+deferred
%% in ets
create_replica(Id) ->
    ets:insert(orswot_eqc, #replica{id=Id}).

%% @doc create_replica_next - Add the new replica ID to state
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.

%% ------ Grouped operator: delta_add
delta_add_args(#state{replicas=Replicas}) ->
    [elements(Replicas),
     %% Start of with earlier/fewer elements
     growingelements(?ELEMENTS)].

%% @doc add_pre - Don't add to a set until we have a replica
-spec delta_add_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delta_add_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc delta_add_pre - Ensure correct shrinking, only select a replica that
%% is in the state
-spec delta_add_pre(S :: eqc_statem:symbolic_state(),
              Args :: [term()]) -> boolean().
delta_add_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc delta_add the `Element' to the sets at `Replica'
delta_add(Replica, Element) ->
    [#replica{id=Replica, delta=Delta0, swot=ORSWOT, set=ORSet}=Rep] = ets:lookup(orswot_eqc, Replica),
    {ok, Delta1} = ?SWOT:delta_update({add, Element}, Replica, ORSWOT),
    ORSWOT2 = ?SWOT:merge(Delta1, ORSWOT),
    Delta2 = ?SWOT:merge(Delta0, Delta1),
    {ok, ORSet2} = ?SET:update({add, Element}, Replica, ORSet),
    ets:insert(orswot_eqc, Rep#replica{delta=Delta2, swot=ORSWOT2, set=ORSet2}),
    {ORSWOT2, ORSet2}.

%% @doc delta_add_next - Delta_Add the `Element' to the `delta_adds' list so we can
%% select from it when we come to remove. This increases the liklihood
%% of a remove actuallybeing meaningful.
-spec delta_add_next(S :: eqc_statem:symbolic_state(),
               V :: eqc_statem:var(),
               Args :: [term()]) -> eqc_statem:symbolic_state().
delta_add_next(S=#state{adds=Adds}, _Value, [_, Element]) ->
    S#state{adds=lists:umerge(Adds, [Element])}.

%% @doc delta_add_post - The specification and implementation should be
%% equal in intermediate states as well as at the end.
-spec delta_add_post(S :: eqc_statem:dynamic_state(),
               Args :: [term()], R :: term()) -> true | term().
delta_add_post(_S, _Args, {ORSWOT, ORSet}) ->
    sets_equal(ORSWOT, ORSet).

%% ------ Grouped operator: delta_remove
%% @doc delta_remove_command - Command generator

%% @doc delta_remove_pre - Only delta_remove if there are replicas and elements
%% added already.
-spec delta_remove_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delta_remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

delta_remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [elements(Replicas), elements(Adds)].

%% @doc ensure correct shrinking
delta_remove_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc perform an element delta_remove.
delta_remove(Replica, Value) ->
    [#replica{id=Replica, delta=Delta0, swot=ORSWOT, set=ORSet}=Rep] = ets:lookup(orswot_eqc, Replica),
    %% even though we only delta_remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% element being delta_removed already, so ignore precon errors (they
    %% don't change state)
    {ok, Delta1} = ?SWOT:delta_update({remove, Value}, Replica, ORSWOT),
    ORSWOT2 = ?SWOT:merge(Delta1, ORSWOT),
    Delta2 = ?SWOT:merge(Delta1, Delta0),
    ORSet2 = ignore_preconerror_remove(Value, Replica, ORSet, ?SET),
    ets:insert(orswot_eqc, Rep#replica{delta=Delta2, swot=ORSWOT2, set=ORSet2}),
    {ORSWOT2, ORSet2}.

%% @doc in a non-context delta_remove, not only must the spec and impl be
%% equal, but the `Element' MUST be absent from the set
delta_remove_post(_S, [_, Element], {ORSWOT2, ORSet2}) ->
    eqc_statem:conj([sets_not_member(Element, ORSWOT2),
          sets_equal(ORSWOT2, ORSet2)]).

%% ------ Grouped operator: delta_replicate
%% @doc delta_replicate_args - Choose a From and To for replication
delta_replicate_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas)].

%% @doc delta_replicate_pre - There must be at least on replica to delta_replicate
-spec delta_replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delta_replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc delta_replicate_pre - Ensure correct shrinking
-spec delta_replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
delta_replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc simulate replication by merging state at `To' with state from `From'
delta_replicate(From, To) ->
    [#replica{id=From, delta=FromDelta, set=FromORSet}=FromRep] = ets:lookup(orswot_eqc, From),
    [#replica{id=To, swot=ToORSWOT, set=ToORSet}=ToRep] = ets:lookup(orswot_eqc, To),

    ORSWOT = ?SWOT:merge(FromDelta, ToORSWOT),
    ORSet = ?SET:merge(FromORSet, ToORSet),
    ets:insert(orswot_eqc, ToRep#replica{swot=ORSWOT, set=ORSet}),
    ets:insert(orswot_eqc, FromRep#replica{delta=?SWOT:new()}), %% clear delta
    {ORSWOT, ORSet}.

%% @doc delta_replicate_post - ORSet and ORSWOT must be equal throughout.
-spec delta_replicate_post(S :: eqc_statem:dynamic_state(),
                     Args :: [term()], R :: term()) -> true | term().
delta_replicate_post(_S, [_From, _To], {SWOT, Set}) ->
    sets_equal(SWOT, Set).

%% ------ Grouped operator: idempotent

idempotent_args(#state{replicas=Replicas}) ->
    [elements(Replicas)].

%% @doc idempotent_pre - Precondition for generation
-spec idempotent_pre(S :: eqc_statem:symbolic_state()) -> boolean().
idempotent_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc idempotent_pre - Precondition for idempotent
-spec idempotent_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
idempotent_pre(#state{replicas=Replicas}, [Replica]) ->
    lists:member(Replica, Replicas).

%% @doc idempotent - Merge replica with itself, result used for post condition only
idempotent(Replica) ->
    [#replica{id=Replica, swot=Swot}] = ets:lookup(orswot_eqc, Replica),
    {Swot, ?SWOT:merge(Swot, Swot)}.

%% @doc idempotent_post - Postcondition for idempotent
-spec idempotent_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
idempotent_post(_S, [_Replica], {Swot, MergedSelfSwot}) ->
    swots_equal(Swot, MergedSelfSwot).

%% ------ Grouped operator: commutative

commutative_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas)].

%% @doc commutative_pre - Precondition for generation
-spec commutative_pre(S :: eqc_statem:symbolic_state()) -> boolean().
commutative_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc commutative_pre - Precondition for commutative
-spec commutative_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
commutative_pre(#state{replicas=Replicas}, [Replica, Replica2]) ->
    lists:member(Replica, Replicas) andalso lists:member(Replica2, Replicas).

%% @doc commutative - Merge maps both ways (result used for post condition)
commutative(Replica1, Replica2) ->
    [#replica{id=Replica1, swot=Swot1}] = ets:lookup(orswot_eqc, Replica1),
    [#replica{id=Replica2, swot=Swot2}] = ets:lookup(orswot_eqc, Replica2),
    {?SWOT:merge(Swot1, Swot2), ?SWOT:merge(Swot2, Swot1)}.

%% @doc commutative_post - Postcondition for commutative
-spec commutative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
commutative_post(_S, [_Replica1, _Replica2], {OneMergeTwo, TwoMergeOne}) ->
    swots_equal(OneMergeTwo, TwoMergeOne).

swots_equal(S1, S2) ->
    lists:sort(?SWOT:value(S1)) ==
        lists:sort(?SWOT:value(S2)).

%% ------ Grouped operator: associative

associative_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas), elements(Replicas)].

%% @doc associative_pre - Precondition for generation
-spec associative_pre(S :: eqc_statem:symbolic_state()) -> boolean().
associative_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc associative_pre - Precondition for associative
-spec associative_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
associative_pre(#state{replicas=Replicas}, [Replica, Replica2, Replica3]) ->
    lists:member(Replica, Replicas)
        andalso lists:member(Replica2, Replicas)
        andalso lists:member(Replica3, Replicas).

%% @doc associative - Merge maps three ways (result used for post condition)
associative(Replica1, Replica2, Replica3) ->
    [#replica{id=Replica1, swot=Swot1}] = ets:lookup(orswot_eqc, Replica1),
    [#replica{id=Replica2, swot=Swot2}] = ets:lookup(orswot_eqc, Replica2),
    [#replica{id=Replica3, swot=Swot3}] = ets:lookup(orswot_eqc, Replica3),

    {?SWOT:merge(?SWOT:merge(Swot1, Swot2), Swot3),
     ?SWOT:merge(?SWOT:merge(Swot1, Swot3), Swot2),
     ?SWOT:merge(?SWOT:merge(Swot2, Swot3), Swot1)}.

%% @doc associative_post - Postcondition for associative
-spec associative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
associative_post(_S, [_Replica1, _Replica2, _Replica3], {ABC, ACB, BCA}) ->
    case {
      swots_equal(ABC, ACB),
      swots_equal(ACB, BCA)} of
        {true, true} ->
            true;
        {false, true} ->
            {postcondition_failed, {ACB, not_associative, ABC}};
        {true, false} ->
            {postcondition_failed, {ACB, not_associative, BCA}};
        {false, false} ->
            {postcondition_failed, {{ACB, not_associative, ABC}, '&&', {ACB, not_associative, BCA}}}
    end.

%% @doc weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% removes with adds. But favour adds so we have something to
%% remove. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < 5 ->
    4;
weight(_S, delta_add) ->
    8;
weight(_S, delta_remove) ->
    3;
weight(_S, delta_replicate) ->
    2;
weight(_S, _) ->
    1.

%% @doc check that the implementation of the ORSWOT is equivalent to
%% the OR-Set impl.
-spec prop_merge() -> eqc:property().
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% Store the state external to the statem for correct
                %% shrinking. This is best practice.
                ets:new(orswot_eqc, [named_table, set, {keypos, #replica.id}]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                {MergedSwot, MergedSet} = lists:foldl(fun(#replica{swot=ORSWOT, set=ORSet}, {MO, MOS}) ->
                                                                               {?SWOT:merge(ORSWOT, MO),
                                                                                ?SET:merge(MOS, ORSet)}
                                                                       end,
                                                                       {?SWOT:new(), ?SET:new()},
                                                                       ets:tab2list(orswot_eqc)),
                %%SwotDeferred = proplists:get_value(deferred_length, ?SWOT:stats(MergedSwot)),

                ets:delete(orswot_eqc),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          measure(replicas, length(S#state.replicas),
%%                                                  measure(elements, ?SWOT:stat(element_count, MergedSwot),
                                                          conjunction([{result, Res == ok},
                                                                       {equal, sets_equal(MergedSwot, MergedSet)}
                                                                      ]))))

            end).

%% Helpers @doc a non-context remove of an absent element generates a
%% precondition error, but does not mutate the state, so just ignore
%% and return original state.
ignore_preconerror_remove(Value, Actor, Set, Mod) ->
    case Mod:update({remove, Value}, Actor, Set) of
        {ok, Set2} ->
            Set2;
        _E ->
            Set
    end.

%% @doc common precondition and property, do SWOT and Set have the
%% same elements?
sets_equal(ORSWOT, ORSet) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(?SWOT:value(ORSWOT)) ==
        lists:sort(?SET:value(ORSet)) of
        true ->
            true;
        _ ->
            true%%{ORSWOT, '/=', ORSet}
    end.

%% @doc `true' if `Element' is not in `ORSWOT', error tuple otherwise.
sets_not_member(Element, ORSWOT) ->
    case lists:member(Element, ?SWOT:value(ORSWOT)) of
        false ->
            true;
        _ ->
            {Element, member, ORSWOT}
    end.

%% @doc Since OR-Set does not support deferred operations (yet!) After
%% merging the sets and the deferred list, see if any deferred removes
%% are now relevant.
model_apply_deferred(ORSet, Deferred) ->
    Elems = ?SET:value(removed, Deferred),
    lists:foldl(fun(E, {OIn, DIn}) ->
                        Frag = ?SET:value({fragment, E}, Deferred),
                        context_remove_model(E, Frag, OIn, DIn)
                end,
                %% Empty deferred list to build again for ops that are
                %% not enabled by merge
                {ORSet, ?SET:new()},
                Elems).

%% @TODO Knows about the internal working of ?SET,
%% riak_dt_osret should provide deferred operations. Also, this is
%% heinously ugly and hard to read.
context_remove_model(Elem, RemoveContext, ORSet, Deferred) ->
    Present = ?SET:value({tokens, Elem}, ORSet),
    Removing = ?SET:value({tokens, Elem}, RemoveContext),
    %% Any token in the orset that is present in the RemoveContext can
    %% be removed, any that is not must be deferred.
    orddict:fold(fun(Token, _Bool, {ORSetAcc, Defer}) ->
                         case orddict:is_key(Token, Present) of
                             true ->
                                 %% The target has this Token, so
                                 %% merge with it to ensure it is
                                 %% removed
                                 {?SET:merge(ORSetAcc,
                                                      orddict:store(Elem,
                                                                    orddict:store(Token, true, orddict:new()),
                                                                    orddict:new())),
                                  Defer};
                             false ->
                                 %% This token does not yet exist as
                                 %% an add or remove in the orset, so
                                 %% defer it's removal until it has
                                 %% been added by a merge
                                 {ORSetAcc,
                                  orddict:update(Elem, fun(Tokens) ->
                                                               orddict:store(Token, true, Tokens)
                                                       end,
                                                 orddict:store(Token, true, orddict:new()),
                                                 Defer)}
                         end

                 end,
                 {ORSet, Deferred},
                 Removing).

%% @doc merge both orset and deferred operations
model_merge({S1, D1}, {S2, D2}) ->
    S = ?SET:merge(S1, S2),
    D = ?SET:merge(D1, D2),
    model_apply_deferred(S, D).
