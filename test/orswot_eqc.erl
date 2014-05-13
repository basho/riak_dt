%% -------------------------------------------------------------------
%%
%% orswot_eqc: Try and catch bugs crdt_statem could not.
%%
%% TODO DVV disabled? Get, interleave writes, Put
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(orswot_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{replicas=[] :: [binary()], %% Sort of like the ring, upto N*2 ids
               replica_data=[] :: [{ActorId :: binary(),
                                  riak_dt_orswot:orswot(),
                                  model()}],
               %% The data, duplicated values for replicas
               %% Newest at the head of the list.
               %% Prepend only data 'cos symbolic / dynamic state.
               n=0 :: integer(), %% Generated number of replicas
               adds=[] :: [{ActorId :: binary(), atom()}] %% things that have been added
              }).

-type model() :: {riak_dt_orset:orset(), deferred()}.
%% orset remove entries that cannot be added to an orset yet, as the
%% add entry is unseen. A way of modelling the context operations of
%% orswot, without adding the same thing to the ORSet implementation
%% (where it has no real meaning)
-type deferred() :: riak_dt_orset:orset().

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:numtests(1000, ?QC_OUT(prop_merge()))))}.

run() ->
    run(?NUMTESTS).

run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_merge())).

check() ->
    eqc:check(prop_merge()).

%% Initialize the state
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.


%% ------ Grouped operator: set_nr
%% Only set N if N has not been set (ie run once, as the first command)
set_nr_pre(#state{n=N}) ->
     N == 0.

set_nr_args(_S) ->
    [choose(2, 10)].

set_nr(_) ->
    %% Command args used for next state only
    ok.

set_nr_next(S, _V, [N]) ->
    S#state{n=N}.

%% ------ Grouped operator: make_ring
%% Generate a bunch of replicas,
%% only runs until enough are generated
make_ring_pre(#state{replicas=Replicas, n=N}) ->
    N > 0 andalso length(Replicas) < N * 2.

make_ring_args(#state{replicas=Replicas, n=N}) ->
    [Replicas, vector(N, binary(8))].

make_ring(_,_) ->
    %% Command args used for next state only
    ok.

make_ring_next(S, _V, [Replicas, NewReplicas0]) ->
    %% No duplicate replica ids please!
    R1 = lists:umerge(Replicas, NewReplicas0),
    S#state{replicas=R1}.

%% ------ Grouped operator: add
%% Store a new value
add_pre(S) ->
    replicas_ready(S).

add_args(#state{replicas=Replicas, replica_data=ReplicaData}) ->
    [
     oneof(['X', 'Y', 'Z']), %% a new value
     elements(Replicas), % The replica
     ReplicaData %% The existing replica data
    ].

%% Add a value to the set
add(Value, Actor, ReplicaData) ->
    {ORSWOT, {ORSet, Deferred}} = get(Actor, ReplicaData),
    {ok, ORSWOT2} = riak_dt_orswot:update({add, Value}, Actor, ORSWOT),
    {ok, ORSet2} = riak_dt_orset:update({add, Value}, Actor, ORSet),
    {Actor, ORSWOT2, {ORSet2, Deferred}}.

add_next(S=#state{replica_data=ReplicaData, adds=Adds}, Res, [Value, Coord, _]) ->
    %% The state data is prepend only, it grows and grows, but it's based on older state
    %% Newest at the front.
    S#state{replica_data=[Res | ReplicaData], adds=[{Coord, Value} | Adds]}.

add_post(_S, _Args, Res) ->
    post_all(Res, add).

%% ------ Grouped operator: remove
%% remove, but only something that has been added already
remove_pre(S=#state{adds=Adds}) ->
    %% Only do an update if you already did a get
    replicas_ready(S) andalso Adds /= [].

remove_args(#state{adds=Adds, replica_data=ReplicaData}) ->
    [
     elements(Adds), %% Something that has been added
     ReplicaData %% All the vnode data
    ].

remove_pre(#state{adds=Adds}, [Add, _]) ->
    lists:member(Add, Adds).

remove({Replica, Value}, ReplicaData) ->
    {ORSWOT, {ORSet, Deferred}} = get(Replica, ReplicaData),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% element being removed already, so ignore precon errors (they
    %% don't change state)
    ORSWOT2 = ignore_preconerror_remove(Value, ReplicaData, ORSWOT, riak_dt_orswot),
    ORSet2 = ignore_preconerror_remove(Value, ReplicaData, ORSet, riak_dt_orset),
    {Replica, ORSWOT2, {ORSet2, Deferred}}.

ignore_preconerror_remove(Value, Actor, Set, Mod) ->
    case Mod:update({remove, Value}, Actor, Set) of
        {ok, Set2} ->
            Set2;
        _E ->
            Set
    end.

remove_next(S=#state{replica_data=ReplicaData, adds=Adds}, Res, [Add, _]) ->
    S#state{replica_data=[Res | ReplicaData], adds=lists:delete(Add, Adds)}.

remove_post(_S, _Args, Res) ->
    post_all(Res, remove).

%% ------ Grouped operator: replicate
%% Merge two replicas' values
replicate_pre(S=#state{replica_data=ReplicaData}) ->
    replicas_ready(S) andalso ReplicaData /= [].

replicate_args(#state{replicas=Replicas, replica_data=ReplicaData}) ->
    [
     elements(Replicas), %% Replicate from
     elements(Replicas), %% replicate too
     ReplicaData
    ].

%% Don't replicate to oneself
replicate_pre(_S, [VN, VN, _]) ->
    false;
replicate_pre(_S, [_VN1, _VN2, _]) ->
    true.

%% Mutating multiple elements in replica_data in place is bad idea
%% (symbolic vs dynamic state), so instead of treating add/remove and
%% replicate as the same action, this command handles the replicate
%% part. Data from some random replica (From) is replicated to some
%% random replica (To)
replicate(From, To, ReplicaData) ->
    {FromORSWOT, {FromORSet, FromDeferred}} = get(From, ReplicaData),
    {ToORSWOT, {ToORSet, ToDeferred}} = get(To, ReplicaData),
    ORSWOT = riak_dt_orswot:merge(FromORSWOT, ToORSWOT),
    ORSet0 = riak_dt_orset:merge(FromORSet, ToORSet),
    Deferred0 = riak_dt_orset:merge(FromDeferred, ToDeferred),
    {ORSet, Deferred} = model_apply_deferred(ORSet0, Deferred0),
    {To, ORSWOT, {ORSet, Deferred}}.

%% After merging the sets and the deferred list, see if any deferred
%% removes are now relevant
model_apply_deferred(ORSet, Deferred) ->
    Elems = riak_dt_orset:value(removed, Deferred),
    lists:foldl(fun(E, {OIn, DIn}) ->
                        Frag = riak_dt_orset:value({fragment, E}, Deferred),
                        context_remove_model(E, Frag, OIn, DIn)
                end,
                %% Empty deferred list to build again for ops that are
                %% not enabled by merge
                {ORSet, riak_dt_orset:new()},
                Elems).

replicate_next(S=#state{replica_data=ReplicaData}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData]}.

replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% ------ Grouped operator: context_remove
%% Removal operation with a context
context_remove_pre(S=#state{replica_data=ReplicaData}) ->
    replicas_ready(S) andalso ReplicaData /= [].

context_remove_args(#state{replicas=Replicas, replica_data=ReplicaData}) ->
    [
     elements(Replicas), %% read from
     elements(Replicas), %% send op too
     ReplicaData
    ].

%% Don't send ctx ops to oneself
context_remove_pre(_S, [VN, VN, _]) ->
    false;
context_remove_pre(_S, [_VN1, _VN2, _]) ->
    true.

%% checks that a context remove is equivalent to performing a remove
%% on some state and merging it with some other state (eventually) For
%% the model we chose an element, and send it, them remove it, from
%% the target replica (like a partial merge.) If we did a full merge
%% post conditions would not hold.
context_remove(From, To, ReplicaData) ->
    {FromORSWOT, {FromORSet, FromDeferred}} = get(From, ReplicaData),
    {ToORSWOT, {ToORSet, ToDeferred}} = get(To, ReplicaData),
    case choose_element(FromORSWOT, FromORSet) of
        empty ->
            %% No-op
            {From, FromORSWOT, {FromORSet, FromDeferred}};
        {{Ctx, Element}, Fragment} ->
            {ok, ORSWOT} = riak_dt_orswot:update({remove, Element}, To, ToORSWOT, Ctx),
            {ORSet, Deferred} = context_remove_model(Element, Fragment, ToORSet, ToDeferred),
            {To, ORSWOT, {ORSet, Deferred}}
    end.

%% Don't know how to make this a quickcheck generated thing
choose_element(ORSWOT, ORSet) ->
    Elems = riak_dt_orswot:value(ORSWOT),
    %% any element in ORSWOT must be ORSet (see post conditions)
    case Elems of
        [] ->
            empty;
        _->
            Elem = lists:nth(crypto:rand_uniform(1, length(Elems) + 1), Elems),
            Ctx = riak_dt_orswot:precondition_context(ORSWOT),
            Fragment = riak_dt_orset:value({fragment, Elem}, ORSet),
            {{Ctx, Elem}, Fragment}
    end.

%% @TODO Knows about the internal working of riak_dt_orset, maybe
%% model should be a re-impl of that module instead. Also, this is
%% heinously ugly and hard to read.
context_remove_model(Elem, RemoveContext, ORSet, Deferred) ->
    Present = riak_dt_orset:value({tokens, Elem}, ORSet),
    Removing = riak_dt_orset:value({tokens, Elem}, RemoveContext),
    %% Any token in the orset that is present in the RemoveContext can
    %% be removed, any that is not must be deferred.
    orddict:fold(fun(Token, _Bool, {ORSetAcc, Defer}) ->
                         case orddict:is_key(Token, Present) of
                             true ->
                                 %% The target has this Token, so
                                 %% merge with it to ensure it is
                                 %% removed
                                 {riak_dt_orset:merge(ORSetAcc,
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

context_remove_next(S=#state{replica_data=ReplicaData}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData]}.

context_remove_post(_S, _Args, Res) ->
    post_all(Res, ctx_rem).

%% Tests the property that an ORSWOT is equivalent to an ORSet
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S=#state{replicas=Replicas, replica_data=ReplicaData}, Res} = run_commands(?MODULE,Cmds),
                %% Check that collapsing all values leads to the same results for ORSWOT and ORSet
                {{OV, DeferredLength}, {ORV, Def}} = case Replicas of
                                                         [] ->
                                                             {{[], 0}, {[], []}};
                                                         _L ->
                                                             %% Get ALL actor's values
                                                             {OS, {ORSet, Deferred}} = lists:foldl(fun(Actor, {O, {OR, D}}) ->
                                                                                                 {O1, {OR1, D1}} = get(Actor, ReplicaData),
                                                                                                 {riak_dt_orswot:merge(O, O1),
                                                                                                  model_merge({OR, D}, {OR1, D1})} end,
                                                                                         {riak_dt_orswot:new(), {riak_dt_orset:new(), riak_dt_orset:new()}},
                                                                                         Replicas),
                                                             DL = proplists:get_value(deferred_length, riak_dt_orswot:stats(OS)),
                                                             {{riak_dt_orswot:value(OS), DL}, {riak_dt_orset:value(ORSet), Deferred}}
                                                     end,
                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE,Cmds, {H,S,Res},
                                          conjunction([{result,  equals(Res, ok)},
                                                       {values, equals(lists:sort(OV), lists:sort(ORV))},
                                                       {m_ec, equals(length(Def), 0)},
                                                       {ec, equals(DeferredLength, 0)}])
                                         ))
            end).

%% -----------
%% Helpers
%% ----------
model_merge({S1, D1}, {S2, D2}) ->
    S = riak_dt_orset:merge(S1, S2),
    D = riak_dt_orset:merge(D1, D2),
    model_apply_deferred(S, D).

replicas_ready(#state{replicas=Replicas, n=N}) ->
    length(Replicas) >= N andalso N > 0.

post_all({_, ORSWOT, {ORSet, _D}}, Cmd) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(riak_dt_orswot:value(ORSWOT)) == lists:sort(riak_dt_orset:value(ORSet)) of
        true ->
            true;
        _ ->
            {postcondition_failed, "SWOT and Set don't match", Cmd, ORSWOT, ORSet}
    end.


%% if a replica does not yet have replica data, return `new()` for the
%% ORSWOT and ORSet
get(Replica, ReplicaData) ->
    case lists:keyfind(Replica, 1, ReplicaData) of
        {Replica, ORSWOT, ORSet} ->
            {ORSWOT, ORSet};
        false -> {riak_dt_orswot:new(), {riak_dt_orset:new(), riak_dt_orset:new()}}
    end.

-endif. % EQC
