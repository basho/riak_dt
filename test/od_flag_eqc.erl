%% -------------------------------------------------------------------
%%
%% map_eqc: Drive out the merge bugs the other statem couldn't
%%
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

-module(od_flag_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-type model() :: {Cntr :: pos_integer(), %% Unique ID per operation
                      Enables :: set(), %% enables
                      Disable :: set(), %% Tombstones
                      %% Disable that are waiting for enables before they
                      %% can be run (like context ops in the
                      %% riak_dt_od_flag)
                      Deferred :: set()
                     }.

-record(state,{replicas=[] :: [binary()], %% Sort of like the ring, upto N*2 ids
               replica_data=[] :: [{ActorId :: binary(),
                                    riak_dt_od_flag:od_flag(),
                                    model()}],
               n=0 :: pos_integer(), %% Generated number of replicas
               counter=1 :: pos_integer() %% a unique tag per add
              }).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(prop_merge()))))}.

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
set_n_pre(#state{n=N}) ->
     N == 0.

%% Choose how many replicas to have in the system
set_n_args(_S) ->
    [choose(2, 10)].

set_n(_) ->
    %% Command args used for next state only
    ok.

set_n_next(S, _V, [N]) ->
    S#state{n=N}.

%% ------ Grouped operator: make_ring
%%
%% Generate a bunch of replicas, only runs if N is set, and until
%% "enough" are generated (N*2) is more than enough.
make_ring_pre(#state{replicas=Replicas, n=N}) ->
    N > 0 andalso length(Replicas) < N * 2.

make_ring_args(#state{replicas=Replicas, n=N}) ->
    [Replicas, vector(N, binary(8))].

make_ring(_,_) ->
    %% Command args used for next state only
    ok.

make_ring_next(S=#state{replicas=Replicas}, _V, [_, NewReplicas0]) ->
    %% No duplicate replica ids please!
    NewReplicas = lists:filter(fun(Id) -> not lists:member(Id, Replicas) end, NewReplicas0),
    S#state{replicas=Replicas ++ NewReplicas}.

%% ------ Grouped operator: enable
%% enable the flag
enable_pre(S) ->
    replicas_ready(S).

enable_args(#state{replicas=Replicas, replica_data=ReplicaData, counter=Cnt}) ->
    [
     elements(Replicas), % The replica
     Cnt,
     ReplicaData %% The existing vnode data
    ].


enable(Actor, Cnt, ReplicaData) ->
    {Flag, Model} = get(Actor, ReplicaData),
    {ok, Flag2} = riak_dt_od_flag:update(enable, Actor, Flag),
    {ok, Model2} = model_enable(Cnt, Model),
    {Actor, Flag2, Model2}.

enable_next(S=#state{replica_data=ReplicaData, counter=Cnt}, Res, [_, _, _]) ->
    S#state{replica_data=[Res | ReplicaData], counter=Cnt+1}.

enable_post(_S, _Args, Res) ->
    post_all(Res, enable).

%% ------ Grouped operator: disable

disable_pre(S=#state{}) ->
    replicas_ready(S).

disable_args(#state{replicas=Replicas, replica_data=ReplicaData}) ->
    [
     elements(Replicas),
     ReplicaData %% All the vnode data
    ].

disable(Replica, ReplicaData) ->
    {Flag, Model} = get(Replica, ReplicaData),
    {ok, Flag2} = riak_dt_od_flag:update(disable, Replica, Flag),
    Model2 = model_disable(Model),
    {Replica, Flag2, Model2}.

disable_next(S=#state{replica_data=ReplicaData}, Res, [_, _]) ->
    S#state{replica_data=[Res | ReplicaData]}.

disable_post(_S, _Args, Res) ->
    post_all(Res, disable).

%% ------ Grouped operator: ctx_disable
%% disable, but with a context
ctx_disable_pre(S=#state{replica_data=ReplicaData}) ->
    replicas_ready(S) andalso ReplicaData /= [].

ctx_disable_args(#state{replicas=Replicas, replica_data=ReplicaData}) ->
    [
     elements(Replicas), %% read from
     elements(Replicas), %% send op too
     ReplicaData %% All the vnode data
    ].

%% Should we send ctx ops to originating replica?
ctx_disable_pre(_S, [VN, VN, _]) ->
    false;
ctx_disable_pre(_S, [_VN1, _VN2, _]) ->
    true.

ctx_disable(From, To, ReplicaData) ->
    {FromFlag, FromModel} = get(From, ReplicaData),
    {ToFlag, ToModel} = get(To, ReplicaData),
    Ctx = riak_dt_od_flag:precondition_context(FromFlag),
    {ok, Flag} = riak_dt_od_flag:update(disable, To, ToFlag, Ctx),
    Model = model_ctx_disable(FromModel, ToModel),
    {To, Flag, Model}.

ctx_disable_next(S=#state{replica_data=ReplicaData}, Res, _) ->
    S#state{replica_data=[Res | ReplicaData]}.

ctx_disable_post(_S, _Args, Res) ->
    post_all(Res, ctx_disable).

%% ------ Grouped operator: replicate
%% Merge two replicas' values
replicate_pre(S=#state{replica_data=ReplicaData}) ->
    replicas_ready(S) andalso ReplicaData /= [].

replicate_args(#state{replicas=Replicas, replica_data=ReplicaData}) ->
    [
     elements(Replicas), %% Replicate from
     elements(Replicas), %% Replicate to
     ReplicaData
    ].

%% Don't replicate to oneself
replicate_pre(_S, [VN, VN, _]) ->
    false;
replicate_pre(_S, [_VN1, _VN2, _]) ->
    true.

%% Replicate a CRDT from `From' to `To'
replicate(From, To, ReplicaData) ->
    {FromFlag, FromModel} = get(From, ReplicaData),
    {ToFlag, ToModel} = get(To, ReplicaData),
    Flag = riak_dt_od_flag:merge(FromFlag, ToFlag),
    Model = model_merge(FromModel, ToModel),
    {To, Flag, Model}.

replicate_next(S=#state{replica_data=ReplicaData}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData]}.

replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).


%% Tests the property that an od_flag is equivalent to the flag model
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S=#state{replicas=Replicas, replica_data=ReplicaData}, Res} = run_commands(?MODULE,Cmds),
                %% Check that collapsing all values leads to the same results for flag and the Model
                {FlagValue, ModelValue} = case Replicas of
                                             [] ->
                                                 {[], []};
                                             _L ->
                                                 %% Get ALL actor's values
                                                 {Flag, Model} = lists:foldl(fun(Actor, {F, Mo}) ->
                                                                                    {F1, Mo1} = get(Actor, ReplicaData),
                                                                                    {riak_dt_od_flag:merge(F, F1),
                                                                                     model_merge(Mo, Mo1)} end,
                                                                            {riak_dt_od_flag:new(), model_new()},
                                                                            Replicas),
                                                 {riak_dt_od_flag:value(Flag), model_value(Model)}
                                         end,
                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE,Cmds, {H,S,Res},
                                          conjunction([{result,  equals(Res, ok)},
                                                       {values, equals(FlagValue, ModelValue)}])
                                         ))
            end).

%% -----------
%% Helpers
%% ----------
replicas_ready(#state{replicas=Replicas, n=N}) ->
    length(Replicas) >= N andalso N > 0.

post_all({_, Flag, Model}, Cmd) ->
    %% What matters is that both types have the exact same value.
    case riak_dt_od_flag:value(Flag) == model_value(Model) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Flag and Model don't match", Cmd, Flag, Model}
    end.

%% if a replica does not yet have replica data, return `new()` for the
%% Map and Model
get(Replica, ReplicaData) ->
    case lists:keyfind(Replica, 1, ReplicaData) of
        {Replica, Map, Model} ->
            {Map, Model};
        false -> {riak_dt_od_flag:new(), model_new()}
    end.

%% -----------
%% Model
%% ----------
model_new() ->
    {sets:new(), sets:new(), sets:new()}.

model_enable(Cnt, {Enables, Disables, Deferred}) ->
    {ok, {sets:add_element(Cnt, Enables), Disables, Deferred}}.

model_disable({Enables, Disables, Deferred}) ->
    {Enables, sets:union(Disables, Enables), Deferred}.

model_merge({Enables1, Disables1, Deferred1}, {Enables2, Disables2, Deferred2}) ->
    Enables = sets:union(Enables1, Enables2),
    Disables = sets:union(Disables1, Disables2),
    Deferred = sets:union(Deferred1, Deferred2),
    model_apply_deferred(Enables, Disables, Deferred).

model_apply_deferred(Enables, Disables, Deferred) ->
    D2 = sets:subtract(Deferred, Enables),
    Dis = sets:subtract(Deferred, D2),
    {Enables, sets:union(Dis, Disables), D2}.

model_ctx_disable({FromEnables, _FromDisables, _FromDeferred}, {ToEnables, ToDisables, ToDeferred}) ->
    %% Disable enables that are in both
    Disable = sets:intersection(FromEnables, ToEnables),
    %% Defer those enables not seen in target
    Defer = sets:subtract(FromEnables, ToEnables),
    {ToEnables, sets:union(ToDisables, Disable), sets:union(Defer, ToDeferred)}.

model_value({Enables, Disables, _Deferred}) ->
    Remaining = sets:subtract(Enables, Disables),
    sets:size(Remaining) > 0.

-endif. % EQC
