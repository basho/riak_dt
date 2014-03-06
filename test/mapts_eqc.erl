%% -------------------------------------------------------------------
%%
%% map_eqc: Drive out the merge bugs the other statem couldn't
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

-module(mapts_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-type map_model() :: {Cntr :: pos_integer(), %% Unique ID per operation
                      Adds :: set(), %% Things added to the Map
                      Removes :: set() %% Tombstones
                     }.

-record(state,{replicas=[] :: [binary()], %% Sort of like the ring, upto N*2 ids
               replica_data=[] :: [{ActorId :: binary(),
                                  riak_dt_map:map(),
                                  map_model()}],
               %% The data, duplicated values for replicas
               %% Newest at the head of the list.
               %% Prepend only data 'cos symbolic / dynamic state.
               n=0 :: pos_integer(), %% Generated number of replicas
               counter=0 :: pos_integer(), %% a unique tag per add
               adds=[] :: [{ActorId :: binary(), atom()}] %% things that have been added
              }).

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

make_ring_next(S=#state{replicas=Replicas}, _V, [_, NewReplicas0]) ->
    %% No duplicate replica ids please!
    NewReplicas = lists:filter(fun(Id) -> not lists:member(Id, Replicas) end, NewReplicas0),
    S#state{replicas=Replicas ++ NewReplicas}.

%% ------ Grouped operator: add
%% Add a new field
add_pre(S) ->
    replicas_ready(S).

add_args(#state{replicas=Replicas, replica_data=ReplicaData, counter=Cnt}) ->
    [
     %% a new field (too broad?)
     gen_field(),
     elements(Replicas), % The replica
     Cnt,
     ReplicaData %% The existing vnode data
    ].

gen_field() ->
    {oneof(['X,', 'Y', 'Z']),
     oneof([
            riak_dt_pncounter,
            riak_dt_orswot,
            riak_dt_lwwreg,
            riak_dt_tsmap,
            riak_dt_od_flag
           ])}.

gen_field_op({_Name, Type}) ->
    Type:gen_op().

%% Add a Field to the Map
add(Field, Actor, Cnt, ReplicaData) ->
    {Map, Model} = get(Actor, ReplicaData),
    {ok, Map2} = riak_dt_tsmap:update({update, [{add, Field}]}, Actor, Map),
    {ok, Model2} = model_add_field(Field, Cnt, Model),
    {Actor, Map2, Model2}.

add_next(S=#state{replica_data=ReplicaData, adds=Adds, counter=Cnt}, Res, [Field, Actor, _, _]) ->
    %% The state data is prepend only, it grows and grows, but it's based on older state
    %% Newest at the front.
    S#state{replica_data=[Res | ReplicaData], adds=[{Actor, Field} | Adds], counter=Cnt+1}.

add_post(_S, _Args, Res) ->
    post_all(Res, add).

%% ------ Grouped operator: remove
%% remove, but only something that has been added already
remove_pre(S=#state{adds=Adds}) ->
    %% Only do an update if you already did a get
    replicas_ready(S) andalso Adds /= [].

remove_args(#state{adds=Adds, replica_data=ReplicaData}) ->
    [
     elements(Adds), %% A Field that has been added
     ReplicaData %% All the vnode data
    ].

remove_pre(#state{adds=Adds}, [Add, _]) ->
    lists:member(Add, Adds).

remove({Replica, Field}, ReplicaData) ->
    {Map, Model} = get(Replica, ReplicaData),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% Field being removed already, so ignore precon errors (they
    %% don't change state)
    Map2 = ignore_preconerror_remove(Field, Replica, Map),
    Model2 = model_remove_field(Field, Model),
    {Replica, Map2, Model2}.

ignore_preconerror_remove(Field, Actor, Map) ->
    case riak_dt_tsmap:update({update, [{remove, Field}]}, Actor, Map) of
        {ok, Map2} ->
            Map2;
        _ ->
            Map
    end.

remove_next(S=#state{replica_data=ReplicaData, adds=Adds}, Res, [Add, _]) ->
    %% The state data is prepend only, it grows and grows, but it's based on older state
    %% Newest at the front.
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
     elements(Replicas), %% Replicate too
     ReplicaData
    ].

%% Don't replicate to oneself
replicate_pre(_S, [VN, VN, _]) ->
    false;
replicate_pre(_S, [_VN1, _VN2, _]) ->
    true.

%% Replicate a CRDT from `From' to `To'
replicate(From, To, ReplicaData) ->
    {FromMap, FromModel} = get(From, ReplicaData),
    {ToMap, ToModel} = get(To, ReplicaData),
    Map = riak_dt_tsmap:merge(FromMap, ToMap),
    Model = model_merge(FromModel, ToModel),
    {To, Map, Model}.

replicate_next(S=#state{replica_data=ReplicaData}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData]}.

replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% ------ Grouped operator: update
%% Update a Field in the Map
update_pre(S=#state{replica_data=ReplicaData}) ->
    replicas_ready(S) andalso ReplicaData /= [].

update_args(#state{replicas=Replicas, replica_data=ReplicaData, counter=Cnt}) ->
    [
     ?LET(Field, gen_field(), {Field, gen_field_op(Field)}),
     elements(Replicas),
     ReplicaData,
     Cnt
    ].

update({Field, Op}, Replica, ReplicaData, Cnt) ->
    {Map0, Model0} = get(Replica, ReplicaData),
    {ok, Map} = ignore_precon_error(riak_dt_tsmap:update({update, [{update, Field, Op}]}, Replica, Map0), Map0),
    {ok, Model} = model_update_field(Field, Op, Replica, Cnt, Model0),
    {Replica, Map, Model}.

ignore_precon_error({ok, NewMap}, _) ->
    {ok, NewMap};
ignore_precon_error(_, Map) ->
    {ok, Map}.


update_next(S=#state{replica_data=ReplicaData, counter=Cnt}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData], counter=Cnt+1}.

update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% Tests the property that an ORSWOT is equivalent to an ORSet
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S=#state{replicas=Replicas, replica_data=ReplicaData}, Res} = run_commands(?MODULE,Cmds),
                %% Check that collapsing all values leads to the same results for Map and the Model
                {MapValue, ModelValue} = case Replicas of
                                             [] ->
                                                 {[], []};
                                             _L ->
                                                 %% Get ALL actor's values
                                                 {Map, Model} = lists:foldl(fun(Actor, {M, Mo}) ->
                                                                                    {M1, Mo1} = get(Actor, ReplicaData),
                                                                                    %% io:format("~nXXXX DUMP XXXX~nMap:::: ~p~nModel:::: ~p~n", [M1, Mo1]),
                                                                                    {riak_dt_tsmap:merge(M, M1),
                                                                                     model_merge(Mo, Mo1)} end,
                                                                            {riak_dt_tsmap:new(), model_new()},
                                                                            Replicas),
                                                 %% {Clock, V} = Map,
                                                 %% io:format("~nXXXX FINAL XXXX~nMap:::: ~p~nSize:::~p~n", [Clock, length(V)]),
                                                 {riak_dt_tsmap:value(Map), model_value(Model)}
                                         end,
                aggregate(command_names(Cmds),
                          pretty_commands(?MODULE,Cmds, {H,S,Res},
                                          conjunction([{result,  equals(Res, ok)},
                                                       {values, equals(lists:sort(MapValue), lists:sort(ModelValue))}])
                                         ))
            end).

%% -----------
%% Helpers
%% ----------
replicas_ready(#state{replicas=Replicas, n=N}) ->
    length(Replicas) >= N andalso N > 0.

post_all({_, Map, Model}, Cmd) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(riak_dt_tsmap:value(Map)) == lists:sort(model_value(Model)) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Map and Model don't match", Cmd, Map, Model}
    end.


%% if a replica does not yet have replica data, return `new()` for the
%% ORSWOT and ORSet
get(Replica, ReplicaData) ->
    case lists:keyfind(Replica, 1, ReplicaData) of
        {Replica, Map, Model} ->
            {Map, Model};
        false -> {riak_dt_tsmap:new(), model_new()}
    end.


%% -----------
%% Model
%% ----------
model_new() ->
    {sets:new(), sets:new()}.

model_add_field({_Name, Type}=Field, Cnt, {Adds, Removes}) ->
    %% Adding a field that is already present merges with the field
%%    InMap = sets:subtract(Adds, Removes),
    %% CRDT = lists:foldl(fun({{FName, FType}, Value, _X}, Acc) when FName == Name,
    %%                                                               FType == Type ->
    %%                            Type:merge(Acc, Value);
    %%                       (_, Acc) -> Acc
    %%                    end,
    %%                    Type:new(),
    %%                    sets:to_list(InMap)),
    {ok, {sets:add_element({Field, Type:new(), Cnt}, Adds), Removes}}.

model_update_field({Name, Type}=Field, Op, Actor, Cnt, {Adds, Removes}=Model) ->
    InMap = sets:subtract(Adds, Removes),
    CRDT = lists:foldl(fun({{FName, FType}, Value, _X}, Acc) when FName == Name,
                                                                  FType == Type ->
                               Type:merge(Acc, Value);
                          (_, Acc) -> Acc
                       end,
                       Type:new(),
                       sets:to_list(InMap)),
    case Type:update(Op, {Actor, Cnt}, CRDT) of
        {ok, Updated} ->
            {ok, {sets:add_element({Field, Updated, Cnt}, Adds), Removes}};
        _ ->
            {ok, Model}
    end.

model_remove_field(Field, {Adds, Removes}) ->
    ToRemove = [{F, Val, Token} || {F, Val, Token} <- sets:to_list(Adds), F == Field],
    {Adds, sets:union(Removes, sets:from_list(ToRemove))}.

model_merge({Adds1, Removes1}, {Adds2, Removes2}) ->
    {sets:union(Adds1, Adds2), sets:union(Removes1, Removes2)}.

model_value({Adds, Removes}) ->
    Remaining = sets:subtract(Adds, Removes),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res)].

-endif. % EQC
