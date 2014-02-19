%% -------------------------------------------------------------------
%%
%% crdt_statem2_eqc: Quickcheck statem test for riak_dt modules
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

-module(crdt_statem2_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-export([run/2]).
-behaviour(eqc_statem).
-export([initial_state/0, command/1, precondition/2, next_state/3, postcondition/3]).

-record(state,{
    mod, % Module Under Test
    actor_id = 0, % Current Actor
    replicas = [] % List of replicas
    }).

%% TODO: check
-define(NUMTESTS, 1000).
-define(NUMCOMMANDS, 5).
-define(BATCHSIZE, 100).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-type crdt_model() :: term().

%% TODO: apparently callbacks can't overlap. :( find a new name, or something?
%-callback gen_op() -> eqc_gen:gen(riak_dt:operation()).

-callback crdt_model_create() -> crdt_model().
-callback crdt_model_update(riak_dt:operation(), riak_dt:actor(), crdt_model()) -> crdt_model().
-callback crdt_model_merge(crdt_model(), crdt_model()) -> crdt_model().
-callback crdt_model_realise(crdt_model()) -> term().

run(Module,NumTests) ->
    {atom_to_list(Module),
    {timeout, 1200,
     ?_assert(eqc:quickcheck(
              eqc:numtests(NumTests, 
                           ?QC_OUT(prop_crdt_correct(Module)))))}}.
%%% Statem Callbacks

initial_state() ->
    #state{}.

%% Command generator
command(State=#state{mod=Mod, replicas=Replicas}) ->
    % - Create Replica
    % - Update Replica
    % - Merging 2 Replicas
    % - serialize to_binary and deserialize from_binary to check value remains.
    % - GC Ready? & Proposing a GC (getting a fragment)
    % - Exectuting a GC (removing the fragment from a given replica, sometime later)
    frequency(
        [{1, {call, ?MODULE, create, [Mod]}}] ++
        [{10, gen_update(State)} || length(Replicas) > 0] ++
        [{10, {call, ?MODULE, merge,  [Mod, elements(Replicas), elements(Replicas)]}} || length(Replicas) > 0] ++
        [{10, {call, ?MODULE, serialize, [Mod, elements(Replicas)]}} || length(Replicas) > 0]
    ).

gen_update(#state{mod=Mod,replicas=Replicas}) ->
    ?LET(Operations,
         ?SIZED(BatchSize, resize(BatchSize*10, list(Mod:gen_op()))),
         {call, ?MODULE, update, [Mod, Operations, elements(Replicas)]}).


%% Precondition, checked before command is added to the command sequence
precondition(#state{replicas=Replicas}, {call, ?MODULE, update, [_,_,ReplicaTriple]}) ->
    lists:member(ReplicaTriple, Replicas);
precondition(#state{replicas=Replicas}, {call, ?MODULE, merge, [_, ReplicaTriple1, ReplicaTriple2]}) ->
    lists:member(ReplicaTriple1, Replicas) andalso lists:member(ReplicaTriple2, Replicas);
precondition(#state{replicas=Replicas}, {call, ?MODULE, serialize, [_, ReplicaTriple]}) ->
    lists:member(ReplicaTriple, Replicas);
precondition(_S,_Command) ->
    true.

%% Postcondition, checked after command has been evaluated
postcondition(_S, {call, ?MODULE, update, [Mod, Operations, {AId, SymbState, _}]}, Updated) ->
    CRDTVal = value(Mod, Updated),
    SymbState1 = apply_operations(AId, Operations, SymbState, fun Mod:crdt_model_update/3),
    SymbVal = Mod:crdt_model_realise(SymbState1),
    case values_equal(CRDTVal, SymbVal) of
        true -> true;
        _    -> {postcondition_failed, "The Symbolic Value seems to have deviated from the CRDT Value", SymbVal, CRDTVal}
    end;

postcondition(_S, {call, ?MODULE, merge, [Mod, ReplicaTriple1, ReplicaTriple2]}, Merged1) ->
    % In operation ?MODULE:merge, ReplicaTriple1 is merged with ReplicaTriple2.
    % Let's do it the opposite way around to check commutativity
    Merged2 = merge(Mod, ReplicaTriple2, ReplicaTriple1),
    case equal(Mod, Merged1, Merged2) of
        true -> true;
        _    -> {postcondition_failed, "merge/2 is not commutative (in the eyes of equals/2)", Merged1, Merged2}
    end;

postcondition(_S, {call, ?MODULE, serialize, [Mod, {_AId, _SymbState, PreCRDT}]}, PostCRDT) ->
    case equal(Mod, PreCRDT, PostCRDT) of
        true -> true;
        _    -> {postcondition_failed, "binary serialisation changes CRDT internal state", PreCRDT, PostCRDT}
    end;

postcondition(_S,_Command,_CommandRes) ->
    true.

%% Next state transformation, S is the current state
next_state(State = #state{replicas=Replicas, actor_id=AId}, V, {call, ?MODULE, create, [Mod]}) ->
    Replicas1 = [{AId, Mod:crdt_model_create(), V} | Replicas],
    State#state{replicas=Replicas1, actor_id=AId+1};

next_state(State = #state{replicas=Replicas}, V, {call, ?MODULE, update, [Mod, Operations, {AId, SymbState, _DynamicState}]}) ->
    {value, _, Replicas1} = lists:keytake(AId, 1, Replicas),
    SymbState1 = apply_operations(AId, Operations, SymbState, fun Mod:crdt_model_update/3),
    State#state{replicas=[{AId, SymbState1, V}|Replicas1]};

next_state(State = #state{replicas=Replicas}, V,
           {call, ?MODULE, merge, [Mod, {AId1, SymbState1, _DS1}, {_AId2, SymbState2, _DS2}]}) ->
    Replicas1 = lists:keydelete(AId1, 1, Replicas),
    MergedSymbState = Mod:crdt_model_merge(SymbState1, SymbState2),
    NewVal = {AId1, MergedSymbState, V},
    State#state{replicas=[NewVal|Replicas1]};

next_state(S, _V, _Command) ->
    S.

%%% Properties

prop_crdt_correct(Mod) ->
    ?FORALL(Cmds,more_commands(?NUMCOMMANDS,commands(?MODULE,#state{mod=Mod})),
        begin
            Res={_,_,Result} = run_commands(?MODULE, Cmds),
            aggregate(with_title("Command Names"),
                command_names(Cmds),
                pretty_commands(?MODULE,
                    Cmds,
                    Res,
                    eqc_statem:show_states(Result == ok)))
        end).

%%% Callbacks Used Above

create(Module) ->
    Module:new().

update(Module, Operations, {Actor,_SymbState,CRDT}) ->
    apply_operations(Actor, Operations, CRDT, fun Module:update/3).

merge(Module, {_,_,CRDT1},{_,_,CRDT2}) ->
    Module:merge(CRDT1, CRDT2).

serialize(Module, {_,_,CRDT}) ->
    Binary = Module:to_binary(CRDT),
    Module:from_binary(Binary).

value(Module, CRDT) ->
    Module:value(CRDT).

equal(Module, CRDT1, CRDT2) ->
    Module:equal(CRDT1, CRDT2) andalso Module:equal(CRDT2, CRDT1).

% TODO: cope with {ok, crdt()} | {error, term()} in here.
apply_operations(Actor,Operations,Data,Function) ->
    lists:foldl(fun(Operation,Data1) ->
            {ok, Res} = Function(Operation,Actor,Data1),
            Res
        end, Data, Operations).

%%% Priv

values_equal(A,B) when is_list(A) ->
    lists:sort(A) == lists:sort(B);
values_equal(A,B) ->
    A == B.


-endif. % EQC
