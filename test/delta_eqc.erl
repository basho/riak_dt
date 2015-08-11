%% -------------------------------------------------------------------
%%
%% delta_eqc:
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

-module(delta_eqc).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(SET, riak_dt_delta_orswot).
-define(MAX_REPLICAS, 10).

%% The set of possible elements in the set
-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-record(state, {
          replicas=[], %% Actor Ids for replicas in the system
          adds=[],      %% Elements that have been added to the set
          deltas=[],     %% Deltas to merge
          delivered=[] %% deltas delivered
         }).

-record(replica, {
          id,
          set=?SET:new(),
          deltas=?SET:new()
         }
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
    length(Replicas) < ?MAX_REPLICAS.

%% @doc create_replica_command - Command generator
create_replica_args(_S) ->
    [noshrink(char())].

%% @doc create_replica_pre - don't create a replica that already
%% exists
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc create a new replica, and store a bottom orswot
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
%% @doc add_pre - Don't add to a set until we have a replica
-spec delta_add_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delta_add_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

delta_add_args(#state{replicas=Replicas}) ->
    [elements(Replicas),
     %% Start of with earlier/fewer elements
     growingelements(?ELEMENTS)].

%% @doc delta_add_pre - Ensure correct shrinking, only select a replica that
%% is in the state
-spec delta_add_pre(S :: eqc_statem:symbolic_state(),
              Args :: [term()]) -> boolean().
delta_add_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc delta_add the `Element' to the sets at `Replica'
delta_add(Replica, Element) ->
    [#replica{set=Set}=Rep] = ets:lookup(orswot_eqc, Replica),
    {ok, Delta} = ?SET:delta_update({add, Element}, Replica, Set),
    Set2 = ?SET:merge(Delta, Set),
    ets:insert(orswot_eqc, Rep#replica{set=Set2}),
    Delta.

-spec delta_add_next(S :: eqc_statem:symbolic_state(),
               V :: eqc_statem:var(),
               Args :: [term()]) -> eqc_statem:symbolic_state().
delta_add_next(S=#state{adds=Adds, deltas=Deltas}, Delta, [_Replica, Element]) ->
    S#state{adds=lists:umerge(Adds, [Element]), deltas=[Delta | Deltas]}.

%% ------ Grouped operator: delta_remove

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

%% @doc a dynamic precondition uses concrete state, check that the
%% `From' set contains `Element'
delta_remove_dynamicpre(_S, [Replica, Element]) ->
    [#replica{set=Set}] = ets:lookup(orswot_eqc, Replica),
    lists:member(Element, riak_dt_delta_orswot:value(Set)).

%% @doc perform an element delta_remove.
delta_remove(Replica, Element) ->
    [#replica{set=Set}=Rep] = ets:lookup(orswot_eqc, Replica),
    {ok, Delta} = ?SET:delta_update({remove, Element}, Replica, Set),
    Set2 = ?SET:merge(Delta, Set),
    ets:insert(orswot_eqc, Rep#replica{set=Set2}),
    Delta.

delta_remove_next(S=#state{deltas=Deltas}, Delta, [_Replica, _Element]) ->
    S#state{deltas=[Delta | Deltas]}.


%% ------ Grouped operator: delta_replicate
%% @doc delta_replicate_args - Choose a From and To for replication
delta_replicate_args(#state{replicas=Replicas, deltas=Deltas}) ->
    [subset(Deltas), elements(Replicas), elements(Replicas)].

-spec delta_replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
delta_replicate_pre(#state{deltas=Deltas, replicas=Replicas}) ->
    Replicas /= [] andalso Deltas /= [].

%% @doc delta_replicate_pre - Ensure correct shrinking
-spec delta_replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
delta_replicate_pre(#state{replicas=Replicas, deltas=Deltas}, [DeltaBuffer, From, To]) ->
    sets:is_subset(sets:from_list(DeltaBuffer), sets:from_list(Deltas))
        andalso
        lists:member(To, Replicas)
        andalso
        lists:member(From, Replicas).

%% @doc simulate replication by merging state at `To' with `Delta'
delta_replicate(DeltaBuffer, From, To) ->
    [#replica{id=To, set=ToSet0, deltas=ToDelta0}=ToRep] = ets:lookup(orswot_eqc, To),
    [#replica{id=From, set=FromSet}] = ets:lookup(orswot_eqc, From),

    ToDelta = lists:foldl(fun(Delta, Set) ->
                                  ?SET:merge(Delta, Set)
                          end,
                          ToDelta0,
                          DeltaBuffer),
    ToSet = ?SET:merge(FromSet, ToSet0),
    ets:insert(orswot_eqc, ToRep#replica{set=ToSet, deltas=ToDelta}),
    DeltaBuffer.

delta_replicate_next(S=#state{delivered=Delivered}, DeltaBuffer, [_Element, _From, _To]) ->
    S#state{delivered=[DeltaBuffer | Delivered]}.

%% @doc weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% removes with adds. But favour adds so we have something to
%% remove. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 4 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < ?MAX_REPLICAS ->
    4;
weight(_S, delta_add) ->
    8;
weight(_S, delta_remove) ->
    6;
weight(_S, delta_replicate) ->
    17;
weight(_S, _) ->
    1.

%% @doc check that the implementation of the ORSWOT is equivalent to
%% the OR-Set impl.
-spec prop_merge() -> eqc:property().
prop_merge() ->
    ?FORALL(Cmds, more_commands(2, commands(?MODULE)),
            begin
                %% Store the state external to the statem for correct
                %% shrinking. This is best practice.
                ets:new(orswot_eqc, [named_table, set, {keypos, #replica.id}]),

                {H, S=#state{deltas=Deltas, delivered=Delivered}, Res} = run_commands(?MODULE,Cmds),

                Undelivered = sets:fold(fun(D, Acc)  ->
                                                ?SET:merge(D, Acc)
                                        end,
                                        ?SET:new(),
                                        sets:subtract(sets:from_list(Deltas), sets:from_list(lists:flatten(Delivered)))),

                {Sets, DeltaSets} = lists:foldl(fun(#replica{deltas=Delta, set=Set}, {Mergedest, DeltaMerged}) ->
                                                        {?SET:merge(Set, Mergedest),
                                                         ?SET:merge(DeltaMerged, Delta)}
                                                end,
                                                {?SET:new(), Undelivered},
                                                ets:tab2list(orswot_eqc)),

                ets:delete(orswot_eqc),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          measure(commands, length(Cmds),
                                                  measure(deltas, length(Deltas),
                                                          measure(undelivered, length(Deltas) - length(lists:flatten(Delivered)),
                                                                  measure(replicas, length(S#state.replicas),
                                                                          conjunction([{result, Res == ok},
                                                                                       {equal, sets_equal(Sets, DeltaSets)}
                                                                                      ])))))))

            end).

sets_equal(S1, S2) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(?SET:value(S1)) ==
        lists:sort(?SET:value(S2)) of
        true ->
            true;
        _ ->
            {S1, '/=', S2}
    end.

%% @private subset generator, takes a random subset of the given set,
%% in our case a delta buffer, that some, none, or all of, will be
%% flushed.
subset(Set) ->
    ?LET(Keep, vector(length(Set), bool()),
         return([ X || {X, true}<-lists:zip(Set, Keep)])).
-endif.
