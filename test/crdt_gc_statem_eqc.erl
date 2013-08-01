%% -------------------------------------------------------------------
%%
%% crdt_gc_statem_eqc: Quickcheck statem test for riak_dt_gc modules
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(crdt_gc_statem_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{
    mod, % Module Under Test
    actor_id = 0, % Current Actor
    replicas = [], % List of replicas
    fragments = [] % Log of fragments to catchup
    }).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%% Initialize the state
initial_state() ->
    #state{}.

%% Command generator, S is the state
command(#state{mod=Mod, replicas=Replicas}) ->
    % - Create Replica
    % - Update Replica
    % - Merging 2 Replicas
    % ----
    % - GC Ready?
    % - Proposing a GC (getting a fragment)
    % - Exectuting a GC (removing the fragment from a given replica)
    %  - Execute a GC immediately that it's proposed
    %  - Execute sometime later
    oneof(
        [{call, ?MODULE, create, [Mod]}] ++
        [{call, ?MODULE, update, [Mod, Mod:gen_op(), elements(Replicas)]} || length(Replicas) > 0] ++
        [{call, ?MODULE, merge,  [Mod, elements(Replicas), elements(Replicas)]} || length(Replicas) > 0]
    ).

%% Precondition, checked before command is added to the command sequence
precondition(#state{replicas=Replicas}, {call, ?MODULE, update, [_, _, ReplicaTriple]}) ->
    lists:member(ReplicaTriple, Replicas);
precondition(#state{replicas=Replicas}, {call, ?MODULE, merge, [_, ReplicaTriple1, ReplicaTriple2]}) ->
    lists:member(ReplicaTriple1, Replicas) andalso lists:member(ReplicaTriple2, Replicas);
precondition(_S,_Command) ->
    true.

%% Next state transformation, S is the current state
next_state(State = #state{replicas=Replicas, actor_id=AId}, V, {call, ?MODULE, create, [Mod]}) ->
    Replicas1 = [{AId, Mod:create_gc_expected(), V} | Replicas],
    State#state{replicas=Replicas1, actor_id=AId+1};

next_state(State = #state{replicas=Replicas}, V, {call, ?MODULE, update, [Mod, Operation, {AId, SymbState, _ConcreteState}]}) ->
    {value, _, Replicas1} = lists:keytake(AId, 1, Replicas),
    NewVal = {AId, Mod:update_gc_expected(Operation, AId, SymbState), V},
    State#state{replicas=[NewVal|Replicas1]};

next_state(State = #state{replicas=Replicas}, V,
           {call, ?MODULE, merge, [Mod, {AId1, SymbState1, _CS1}, {_AId2, SymbState2, _CS2}]}) ->
    {value, _, Replicas1} = lists:keytake(AId1, 1, Replicas),
    MergedSymbState = Mod:merge_gc_expected(SymbState1, SymbState2),
    NewVal = {AId1, MergedSymbState, V},
    State#state{replicas=[NewVal|Replicas1]};

next_state(S, _V, _Command) ->
    S.

%% Postcondition, checked after command has been evaluated
postcondition(_S, {call, ?MODULE, update, [Mod, Operation, {AId, SymbState, _}]}, Updated) ->
    CRDTVal = value(Mod,Updated),
    SymbState1 = Mod:update_gc_expected(Operation, AId, SymbState),
    SymbVal = Mod:realise_gc_expected(SymbState1),
    case values_equal(CRDTVal, SymbVal) of
        true -> true;
        _    -> {postcondition_failed, "SymbVal does not look like CRDTVal", SymbVal, CRDTVal}
    end;

postcondition(_S, {call, ?MODULE, merge, [Mod, ReplicaTriple1, ReplicaTriple2]}, Merged1) ->
    % In operation ?MODULE:merge, ReplicaTriple1 is merged with ReplicaTriple2.
    % Let's do it the opposite way around to check commutativity
    Merged2 = merge(Mod, ReplicaTriple2, ReplicaTriple1),
    case equal(Mod, Merged1, Merged2) of
        true -> true;
        _    -> {postcondition_failed, "Merge is not commutative", Merged1, Merged2}
    end;

postcondition(_S,_Command,_CommandRes) ->
    true.

prop_gc_correct(Mod) ->
    ?FORALL(Cmds,commands(?MODULE,#state{mod=Mod}),
        begin
            Res={_,_,Result} = run_commands(?MODULE, Cmds),
            pretty_commands(?MODULE, Cmds, Res, Result == ok)
        end).

create(Module) ->
    Module:new().

update(Module, Operation, {Actor,_SymbState,CRDT}) ->
    Module:update(Operation,Actor,CRDT).

merge(Module, {_,_,CRDT1},{_,_,CRDT2}) ->
    Module:merge(CRDT1, CRDT2).

value(Module, CRDT) ->
    Module:value(CRDT).

values_equal(A,B) when is_list(A) ->
    lists:sort(A) == lists:sort(B);
values_equal(A,B) ->
    A == B.

equal(Module, CRDT1, CRDT2) ->
    Module:equal(CRDT1, CRDT2) andalso Module:equal(CRDT2, CRDT1).


-endif. % EQC
