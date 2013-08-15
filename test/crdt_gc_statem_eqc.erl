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
    fragments = [] % Log of fragments to catchup
    }).

-define(NUMTESTS, 1000).
-define(NUMCOMMANDS, 5).
-define(BATCHSIZE, 100).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

-type crdt_model() :: term().
-type meta() :: term().

% -callback gen_op() -> eqc_gen:gen(riak_dt:operation()).
-callback gen_gc_ops() -> eqc_gen:gen([riak_dt:operation(),...]).

-callback gc_model_create() -> crdt_model().
-callback gc_model_update(riak_dt:operation(), riak_dt:actor(), crdt_model()) -> crdt_model().
-callback gc_model_merge(crdt_model(), crdt_model()) -> crdt_model().
-callback gc_model_realise(crdt_model()) -> term().
-callback gc_model_ready(meta(),crdt_model()) -> boolean().

%%% Statem Callbacks

initial_state() ->
    #state{}.

%% Command generator
command(State=#state{mod=Mod, replicas=Replicas, fragments=Fragments}) ->
    % - Create Replica (Y)
    % - Update Replica (Y)
    % - Merging 2 Replicas (Y)
    % ----
    % - GC Ready?
    % - Proposing a GC (getting a fragment)
    % - Exectuting a GC (removing the fragment from a given replica, sometime later)
    frequency(
        [{1, {call, ?MODULE, create, [Mod]}}] ++
        [{10, gen_update(State)} || length(Replicas) > 0] ++
        [{10, gen_gc_update(State)} || length(Replicas) > 0] ++
        [{10, {call, ?MODULE, merge,  [Mod, elements(Replicas), elements(Replicas)]}} || length(Replicas) > 0] ++
        [{10, {call, ?MODULE, gc_prepare, [Mod, gen_meta(State), elements(Replicas)]}} || length(Replicas) > 0] ++
        [{50, gen_gc_execute(State)} || length(Fragments) > 0]
    ).

gen_update(#state{mod=Mod,replicas=Replicas}) ->
    ?LET(Operations,
         shrink_list(lists:duplicate(?BATCHSIZE, Mod:gen_op())),
         {call, ?MODULE, update, [Mod, Operations, elements(Replicas)]}).

gen_gc_update(#state{mod=Mod,replicas=Replicas}) ->
    ?LET(Operations,
         Mod:gen_gc_ops(),
         {call, ?MODULE, update, [Mod, Operations, elements(Replicas)]}).

gen_gc_execute(#state{mod=Mod,replicas=Replicas,fragments=Fragments}) ->
    ?LET(FragInfo={AId,_Meta,_CRDTFrag},
         elements(Fragments),
         begin
             ReplicaTriple = {AId,_,_} = lists:keyfind(AId, 1, Replicas),
             {call, ?MODULE, gc_execute, [Mod, FragInfo, ReplicaTriple]}
         end).

gen_meta(#state{replicas=Replicas}) ->
    Primaries = [ AId || {AId,_,_} <- lists:sublist(Replicas,1,3)],
    Epoch = riak_dt_gc:new_epoch(hd(Primaries)), % Primaries is never []
    riak_dt_gc:meta(Epoch, Primaries, [], 1.0).


%% Precondition, checked before command is added to the command sequence
precondition(#state{replicas=Replicas}, {call, ?MODULE, update, [_,_,ReplicaTriple]}) ->
    lists:member(ReplicaTriple, Replicas);
precondition(#state{replicas=Replicas}, {call, ?MODULE, merge, [_, ReplicaTriple1, ReplicaTriple2]}) ->
    lists:member(ReplicaTriple1, Replicas) andalso lists:member(ReplicaTriple2, Replicas);
precondition(#state{replicas=Replicas}, {call, ?MODULE, gc_prepare, [_, _, ReplicaTriple]}) ->
    lists:member(ReplicaTriple, Replicas);
% precondition(#state{replicas=Replicas}, {call, ?MODULE, gc_execute, [_, _, ReplicaTriple]}) ->
%     lists:member(ReplicaTriple, Replicas);
precondition(_S,_Command) ->
    true.

%% Postcondition, checked after command has been evaluated
postcondition(_S, {call, ?MODULE, update, [Mod, Operations, {AId, SymbState, _}]}, Updated) ->
    CRDTVal = value(Mod,Updated),
    SymbState1 = apply_operations(AId, Operations, SymbState, fun Mod:gc_model_update/3),
    SymbVal = Mod:gc_model_realise(SymbState1),
    case values_equal(CRDTVal, SymbVal) of
        true -> true;
        _    -> {postcondition_failed, "The Symbolic Value seems to have deviated from the CRDT Value", SymbVal, CRDTVal}
    end;

% TODO: Have a merge() that copes with different epochs
postcondition(_S, {call, ?MODULE, merge, [Mod, ReplicaTriple1, ReplicaTriple2]}, Merged1) ->
    % In operation ?MODULE:merge, ReplicaTriple1 is merged with ReplicaTriple2.
    % Let's do it the opposite way around to check commutativity
    Merged2 = merge(Mod, ReplicaTriple2, ReplicaTriple1),
    case equal(Mod, Merged1, Merged2) of
        true -> true;
        _    -> {postcondition_failed, "merge/2 is not commutative (in the eyes of equals/2)", Merged1, Merged2}
    end;

postcondition(_S, {call, ?MODULE, gc_prepare, [Mod, Meta, {_AId, SymbState, _}]}, CRDTReady) ->
    SymbReady = Mod:gc_model_ready(Meta, SymbState),
    case SymbReady =:= CRDTReady of
        true -> true;
        _    -> {postcondition_failed, "eqc_gc_ready/2 does not agree with gc_ready/2", SymbReady, CRDTReady}
    end;

postcondition(_S, {call, ?MODULE, gc_execute, [Mod, _, {_AId2,_SymbState,CRDTBeforeGc}]}, CRDTAfterGc) ->
    BeforeVal = value(Mod,CRDTBeforeGc),
    AfterVal  = value(Mod,CRDTAfterGc),
    case values_equal(BeforeVal,AfterVal) of
        true -> true;
        _    -> {postcondition_failed, "realised value changes after a gc", BeforeVal, AfterVal}
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
    Replicas1 = [{AId, Mod:gc_model_create(), V} | Replicas],
    State#state{replicas=Replicas1, actor_id=AId+1};

next_state(State = #state{replicas=Replicas}, V, {call, ?MODULE, update, [Mod, Operations, {AId, SymbState, _DynamicState}]}) ->
    {value, _, Replicas1} = lists:keytake(AId, 1, Replicas),
    SymbState1 = apply_operations(AId, Operations, SymbState, fun Mod:gc_model_update/3),
    State#state{replicas=[{AId, SymbState1, V}|Replicas1]};

next_state(State = #state{replicas=Replicas}, V,
           {call, ?MODULE, merge, [Mod, {AId1, SymbState1, _DS1}, {_AId2, SymbState2, _DS2}]}) ->
    Replicas1 = lists:keydelete(AId1, 1, Replicas),
    MergedSymbState = Mod:gc_model_merge(SymbState1, SymbState2),
    NewVal = {AId1, MergedSymbState, V},
    State#state{replicas=[NewVal|Replicas1]};

next_state(State = #state{fragments=Fragments}, _V,
           {call, ?MODULE, gc_prepare, [Mod, Meta, {AId, SymbState, CRDTState}]}) ->
    % If were ready to gc, add a fragment to the list of fragments
    SymbReady = Mod:gc_model_ready(Meta, SymbState),
    Fragments1 = case SymbReady of
                    true -> CRDTFrag = get_fragment(Mod, Meta, CRDTState),
                            [{AId,Meta,CRDTFrag}|Fragments];
                    _    -> Fragments
                end,
    State#state{fragments=Fragments1};

% TODO: work out which replicas we can actually execute the gc on, more than just the current one.
next_state(State = #state{fragments=Fragments}, _V,
           {call, ?MODULE, gc_execute, [_Mod, {AId1,_,_}, _]}) ->
    % Remove the current fragment from Fragments, as we've checked it.
    % TODO: afterwards, check any remaining fragments.
    Fragments1 = lists:keydelete(AId1, 1, Fragments),
    State#state{fragments=Fragments1};

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

%%% Callbacks Used Above

create(Module) ->
    Module:new().

update(Module, Operations, {Actor,_SymbState,CRDT}) ->
    apply_operations(Actor, Operations, CRDT, fun Module:update/3).

merge(Module, {_,_,CRDT1},{_,_,CRDT2}) ->
    Module:merge(CRDT1, CRDT2).

gc_prepare(Module, Meta, {_,_,CRDT}) ->
    Module:gc_ready(Meta, CRDT).

gc_execute(_Module, {_,_,symbolic_frag}, {_,_,CRDT}) ->
    CRDT;
gc_execute(Module, {_,Meta,CRDTFrag}, {_,_,CRDT}) ->
    Module:gc_replace_fragment(Meta, CRDTFrag, CRDT).

value(Module, CRDT) ->
    Module:value(CRDT).

equal(Module, CRDT1, CRDT2) ->
    Module:equal(CRDT1, CRDT2) andalso Module:equal(CRDT2, CRDT1).

get_fragment(_Module, _Meta, {var, _}) ->
    symbolic_frag;
get_fragment(Module, Meta, CRDT) ->
    Module:gc_get_fragment(Meta, CRDT).

replace_fragment(Module, Meta, CRDTFrag, CRDT) ->
    Module:gc_replace_fragment(Meta, CRDTFrag, CRDT).

apply_operations(Actor,Operations,Data,Function) ->
    lists:foldl(fun(Operation,Data1) ->
            Function(Operation,Actor,Data1)
        end, Data, Operations).

%%% Priv

values_equal(A,B) when is_list(A) ->
    lists:sort(A) == lists:sort(B);
values_equal(A,B) ->
    A == B.


-endif. % EQC
