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
-behaviour(eqc_statem).
-export([initial_state/0, command/1, precondition/2, next_state/3, postcondition/3]).

-record(state,{
    mod, % Module Under Test
    actor_id = 0, % Current Actor
    replicas = [], % List of replicas
    fragments = [], % Log of fragments to catchup
    gc_readies = orddict:new() % Orddict full of actors ready to GC
    }).

-define(NUMTESTS, 1000).
-define(NUMCOMMANDS, 10000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%%% Statem Callbacks

%% Initialize the state
initial_state() ->
    #state{}.

%% Command generator, S is the state
command(State=#state{mod=Mod, replicas=Replicas, gc_readies=GcReadies}) ->
    % - Create Replica (Y)
    % - Update Replica (Y)
    % - Merging 2 Replicas (Y)
    % ----
    % - GC Ready?
    % - Proposing a GC (getting a fragment)
    % - Exectuting a GC (removing the fragment from a given replica)
    %  - Execute a GC immediately that it's proposed
    %  - Execute sometime later
    case orddict:size(GcReadies) of
        0 -> frequency(
                [{1, {call, ?MODULE, create, [Mod]}}] ++
                [{10, {call, ?MODULE, update, [Mod, Mod:gen_op(), elements(Replicas)]}} || length(Replicas) > 0] ++
                [{1, {call, ?MODULE, merge,  [Mod, elements(Replicas), elements(Replicas)]}} || length(Replicas) > 0] ++
                [{10, {call, ?MODULE, gc_ready, [Mod, gen_meta(State), elements(Replicas)]}} || length(Replicas) > 0]
             );
        _ -> gen_get_fragment(State)
    end.


gen_get_fragment(#state{mod=Mod, replicas=Replicas, gc_readies=GcReadies}) ->
    ?LET({AId,Meta},
         elements(orddict:to_list(GcReadies)),
         begin
             ReplicaTriple = {AId,_,_} = lists:keyfind(AId,1,Replicas),
             io:format("G"),
             {call, ?MODULE, gc_get_fragment, [Mod, Meta, ReplicaTriple]}
         end).

gen_meta(#state{replicas=Replicas}) ->
    Primaries = [ AId || {AId,_,_} <- lists:sublist(Replicas,1,3)],
    Epoch = riak_dt_gc:new_epoch(hd(Primaries)), % Primaries is never []
    riak_dt_gc:meta(Epoch, Primaries, [], 1.0).

%% Precondition, checked before command is added to the command sequence
precondition(#state{replicas=Replicas}, {call, ?MODULE, update, [_, _, ReplicaTriple]}) ->
    lists:member(ReplicaTriple, Replicas);
precondition(#state{replicas=Replicas}, {call, ?MODULE, merge, [_, ReplicaTriple1, ReplicaTriple2]}) ->
    lists:member(ReplicaTriple1, Replicas) andalso lists:member(ReplicaTriple2, Replicas);
precondition(#state{replicas=Replicas}, {call, ?MODULE, gc_ready, [_, _, ReplicaTriple]}) ->
    lists:member(ReplicaTriple, Replicas);
% precondition(#state{gc_readies=GcReadies}, {call, ?MODULE, gc_get_fragment, [_Mod, _Meta, {AId, _SymbState, _CRDTState}]}) ->
%     orddict:is_key(AId, GcReadies);
precondition(_S,_Command) ->
    true.

%% Postcondition, checked after command has been evaluated
postcondition(_S, {call, ?MODULE, update, [Mod, Operation, {AId, SymbState, _}]}, Updated) ->
    CRDTVal = value(Mod,Updated),
    SymbState1 = Mod:update_gc_expected(Operation, AId, SymbState),
    SymbVal = Mod:realise_gc_expected(SymbState1),
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

postcondition(_S, {call, ?MODULE, gc_ready, [Mod, Meta, {_AId, SymbState, _}]}, CRDTReady) ->
    SymbReady = Mod:eqc_gc_ready(Meta, SymbState),
    case SymbReady =:= CRDTReady of
        true -> true;
        _    -> {postcondition_failed, "eqc_gc_ready/2 does not agree with gc_ready/2", SymbReady, CRDTReady}
    end;

postcondition(_S, {call, ?MODULE, gc_get_fragment, [Mod, Meta, {_AId, _SymbState, CRDTState}]}, CRDTFragment) ->
    CRDTVal = value(Mod, CRDTState),
    PostGCCRDT = replace_fragment(Mod, Meta, CRDTFragment, CRDTState),
    PostGCCRDTVal = value(Mod, PostGCCRDT),
    case values_equal(CRDTVal, PostGCCRDTVal) of
        true -> true;
        _    -> {postcondition_failed, "concrete state changes value when GCd", CRDTVal, PostGCCRDTVal}
    end;
    % Four Options (* is the one done above)
    % - Get SymbFrag, apply SymbFrag to SymbState to check value doesn't change
    % - Get symbfrag, apply symbfrag to symbstate to check value stays "in step" with concfrag and concstate
    % * Apply ConcFrag to ConcState to check value doesn't change
    % - Check concfrag with symbstate

postcondition(_S,_Command,_CommandRes) ->
    true.

%% Next state transformation, S is the current state
next_state(State = #state{replicas=Replicas, actor_id=AId}, V, {call, ?MODULE, create, [Mod]}) ->
    Replicas1 = [{AId, Mod:create_gc_expected(), V} | Replicas],
    State#state{replicas=Replicas1, actor_id=AId+1};

next_state(State = #state{replicas=Replicas}, V, {call, ?MODULE, update, [Mod, Operation, {AId, SymbState, _DynamicState}]}) ->
    {value, _, Replicas1} = lists:keytake(AId, 1, Replicas),
    NewVal = {AId, Mod:update_gc_expected(Operation, AId, SymbState), V},
    State#state{replicas=[NewVal|Replicas1]};

next_state(State = #state{replicas=Replicas}, V,
           {call, ?MODULE, merge, [Mod, {AId1, SymbState1, _DS1}, {_AId2, SymbState2, _DS2}]}) ->
    {value, _, Replicas1} = lists:keytake(AId1, 1, Replicas),
    MergedSymbState = Mod:merge_gc_expected(SymbState1, SymbState2),
    NewVal = {AId1, MergedSymbState, V},
    State#state{replicas=[NewVal|Replicas1]};

next_state(State = #state{gc_readies=GcReadies}, _V,
           {call, ?MODULE, gc_ready, [Mod, Meta, {AId, SymbState, _DynamicState}]}) ->
    SymbReady = Mod:eqc_gc_ready(Meta, SymbState),
    GcReadies1 = case SymbReady of
                    true -> orddict:store(AId, Meta, GcReadies);
                    _    -> GcReadies
                 end,
    State#state{gc_readies=GcReadies1};

next_state(State=#state{fragments=Fragments}, Fragment,
           {call, ?MODULE, gc_get_fragment, [Mod, Meta, {_AId, SymbState, _DS}]}) ->
    SymbFrag = Mod:eqc_gc_get_fragment(Meta, SymbState),
    State#state{fragments=[{Meta,SymbFrag,Fragment}|Fragments]};

next_state(S, _V, _Command) ->
    S.

%%% Properties

prop_gc_correct(Mod) ->
    ?FORALL(Cmds,more_commands(?NUMCOMMANDS,commands(?MODULE,#state{mod=Mod})),
        begin
            Res={_,_,Result} = run_commands(?MODULE, Cmds),
            collect(with_title("Command Lengths"),
                length(Cmds),
                aggregate(with_title("Command Names"),
                    command_names(Cmds),
                    pretty_commands(?MODULE,
                        Cmds,
                        Res,
                        eqc_statem:show_states(Result == ok))))
        end).

commands_sampler(Mod) ->
    ?LET(Cmds,more_commands(?NUMCOMMANDS,commands(?MODULE,#state{mod=Mod})),
         length(Cmds)).



%%% Callbacks Used Above

create(Module) ->
    Module:new().

update(Module, Operation, {Actor,_SymbState,CRDT}) ->
    Module:update(Operation,Actor,CRDT).

merge(Module, {_,_,CRDT1},{_,_,CRDT2}) ->
    Module:merge(CRDT1, CRDT2).

gc_ready(Module, Meta, {_,_,CRDT}) ->
    Module:gc_ready(Meta, CRDT).

gc_get_fragment(Module, Meta, {_,_,CRDT}) ->
    Module:gc_get_fragment(Meta,CRDT).

value(Module, CRDT) ->
    Module:value(CRDT).

equal(Module, CRDT1, CRDT2) ->
    Module:equal(CRDT1, CRDT2) andalso Module:equal(CRDT2, CRDT1).

replace_fragment(Module, Meta, CRDTFrag, CRDT) ->
    Module:gc_replace_fragment(Meta, CRDTFrag, CRDT).

%%% Priv

values_equal(A,B) when is_list(A) ->
    lists:sort(A) == lists:sort(B);
values_equal(A,B) ->
    A == B.


-endif. % EQC
