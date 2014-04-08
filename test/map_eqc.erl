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

-module(map_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(model, {
          adds=sets:new() :: set(), %% Things added to the Map
          removes=sets:new() :: set(), %% Tombstones
          %% Removes that are waiting for adds before they
          %% can be run (like context ops in the
          %% riak_dt_map)
          deferred=sets:new() ::set(),
          clock=riak_dt_vclock:fresh() :: riak_dt_vclock:vclock() %% for embedded context operations
         }).

-type map_model() :: #model{}.

-record(state,{replicas=[] :: [binary()], %% Sort of like the ring, upto N*2 ids
               replica_data=[] :: [{ActorId :: binary(),
                                  riak_dt_map:map(),
                                  map_model()}],
               n=0 :: pos_integer(), %% Generated number of replicas
               counter=1 :: pos_integer(), %% a unique tag per add
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

%% ------ Grouped operator: add
%% Add a new field

add_pre(S) ->
    replicas_ready(S).

add_args(#state{replicas=Replicas, replica_data=ReplicaData, counter=Cnt}) ->
    [
     %% a new field
     gen_field(),
     elements(Replicas), % The replica
     Cnt,
     ReplicaData %% The existing vnode data
    ].

%% Keep the number of possible field names down to a minimum. The
%% smaller state space makes EQC more likely to find bugs since there
%% will be more action on the fields. Learned this from
%% crdt_statem_eqc having to large a state space and missing bugs.
gen_field() ->
    {oneof(['X', 'Y', 'Z']),
     oneof([
            riak_dt_pncounter,
            riak_dt_orswot,
            riak_dt_lwwreg,
            riak_dt_map,
            riak_dt_od_flag
           ])}.

gen_field_op({_Name, Type}) ->
    Type:gen_op().

%% Add a Field to the Map
add(Field, Actor, Cnt, ReplicaData) ->
    {Map, Model} = get(Actor, ReplicaData),
    {ok, Map2} = riak_dt_map:update({update, [{add, Field}]}, Actor, Map),
    {ok, Model2} = model_add_field(Field, Actor, Cnt, Model),
    {Actor, Map2, Model2}.

add_next(S=#state{replica_data=ReplicaData, adds=Adds, counter=Cnt}, Res, [Field, Actor, _, _]) ->
    S#state{replica_data=[Res | ReplicaData], adds=[{Actor, Field} | Adds], counter=Cnt+1}.

add_post(_S, _Args, Res) ->
    post_all(Res, add).

%% ------ Grouped operator: remove

%% remove, but only something that has been added already
remove_pre(S=#state{adds=Adds}) ->
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
    {ok, Map2} = ignore_precon_error(riak_dt_map:update({update, [{remove, Field}]}, Replica, Map), Map),
    Model2 = model_remove_field(Field, Model),
    {Replica, Map2, Model2}.

remove_next(S=#state{replica_data=ReplicaData, adds=Adds}, Res, [Add, _]) ->
    S#state{replica_data=[Res | ReplicaData], adds=lists:delete(Add, Adds)}.

remove_post(_S, _Args, Res) ->
    post_all(Res, remove).

%% ------ Grouped operator: ctx_remove
%% remove, but with a context
ctx_remove_pre(S=#state{replica_data=ReplicaData, adds=Adds}) ->
    replicas_ready(S) andalso Adds /= [] andalso ReplicaData /= [].

ctx_remove_args(#state{replicas=Replicas, replica_data=ReplicaData, adds=Adds}) ->
    ?LET({{From, Field}, To}, {elements(Adds), elements(Replicas)},
         [
          From,        %% read from
          To,          %% send op to
          Field,       %% which field to remove
          ReplicaData  %% All the vnode data
         ]).

%% Should we send ctx ops to originating replica?
ctx_remove_pre(_S, [VN, VN, _, _]) ->
    false;
ctx_remove_pre(_S, [_VN1, _VN2, _, _]) ->
    true.

ctx_remove(From, To, Field, ReplicaData) ->
    {FromMap, FromModel} = get(From, ReplicaData),
    {ToMap, ToModel} = get(To, ReplicaData),
    Ctx = riak_dt_map:precondition_context(FromMap),
    {ok, Map} = riak_dt_map:update({update, [{remove, Field}]}, To, ToMap, Ctx),
    Model = model_ctx_remove(Field, FromModel, ToModel),
    {To, Map, Model}.


ctx_remove_next(S=#state{replica_data=ReplicaData}, Res, _) ->
    S#state{replica_data=[Res | ReplicaData]}.

ctx_remove_post(_S, _Args, Res) ->
    post_all(Res, ctx_remove).

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
    {FromMap, FromModel} = get(From, ReplicaData),
    {ToMap, ToModel} = get(To, ReplicaData),
    Map = riak_dt_map:merge(FromMap, ToMap),
    Model = model_merge(FromModel, ToModel),
    {To, Map, Model}.

replicate_next(S=#state{replica_data=ReplicaData}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData]}.

replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% ------ Grouped operator: ctx_update
%% Update a Field in the Map, using the Map context
ctx_update_pre(S) ->
    replicas_ready(S).

ctx_update_args(#state{replicas=Replicas, replica_data=ReplicaData, counter=Cnt}) ->
    [
     ?LET(Field, gen_field(), {Field, gen_field_op(Field)}),
     elements(Replicas),
     elements(Replicas),
     ReplicaData,
     Cnt
    ].

ctx_update({Field, Op}, From,  To, ReplicaData, Cnt) ->
    {CtxMap, CtxModel} = get(From, ReplicaData),
    {ToMap, ToModel} = get(To, ReplicaData),
    Ctx = riak_dt_map:precondition_context(CtxMap),
    ModCtx = model_ctx(CtxModel),
    {ok, Map} = riak_dt_map:update({update, [{update, Field, Op}]}, To, ToMap, Ctx),
    {ok, Model} = model_update_field(Field, Op, To, Cnt, ToModel, ModCtx),
    {To, Map, Model}.

ctx_update_next(S=#state{replica_data=ReplicaData, counter=Cnt}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData], counter=Cnt+1}.

ctx_update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% ------ Grouped operator: update
%% Update a Field in the Map
update_pre(S) ->
    replicas_ready(S).

update_args(#state{replicas=Replicas, replica_data=ReplicaData, counter=Cnt}) ->
    [
     ?LET(Field, gen_field(), {Field, gen_field_op(Field)}),
     elements(Replicas),
     ReplicaData,
     Cnt
    ].

update({Field, Op}, Replica, ReplicaData, Cnt) ->
    {Map0, Model0} = get(Replica, ReplicaData),
    {ok, Map} = ignore_precon_error(riak_dt_map:update({update, [{update, Field, Op}]}, Replica, Map0), Map0),
    {ok, Model} = model_update_field(Field, Op, Replica, Cnt, Model0,  undefined),
    {Replica, Map, Model}.

%% precondition errors don't change the state of a map
ignore_precon_error({ok, NewMap}, _) ->
    {ok, NewMap};
ignore_precon_error(_, Map) ->
    {ok, Map}.


update_next(S=#state{replica_data=ReplicaData, counter=Cnt}, Res, _Args) ->
    S#state{replica_data=[Res | ReplicaData], counter=Cnt+1}.

update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% Tests the property that an Map is equivalent to the Map Model
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
                                                                                    {riak_dt_map:merge(M, M1),
                                                                                     model_merge(Mo, Mo1)} end,
                                                                            {riak_dt_map:new(), model_new()},
                                                                            Replicas),
                                                 {riak_dt_map:value(Map), model_value(Model)}
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
    case lists:sort(riak_dt_map:value(Map)) == lists:sort(model_value(Model)) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Map and Model don't match", Cmd, Map, Model}
    end.


%% if a replica does not yet have replica data, return `new()` for the
%% Map and Model
get(Replica, ReplicaData) ->
    case lists:keyfind(Replica, 1, ReplicaData) of
        {Replica, Map, Model} ->
            {Map, Model};
        false -> {riak_dt_map:new(), model_new()}
    end.


%% -----------
%% Model
%% ----------
model_new() ->
    #model{}.

model_add_field({_Name, Type}=Field, Actor, Cnt, Model) ->
    #model{adds=Adds, clock=Clock} = Model,
    {ok, Model#model{adds=sets:add_element({Field, Type:new(), Cnt}, Adds),
                     clock=riak_dt_vclock:merge([[{Actor, Cnt}], Clock])}}.

model_update_field({_Name, Type}=Field, Op, Actor, Cnt, Model, Ctx) ->
    #model{adds=Adds, removes=Removes, clock=Clock} = Model,
    Clock2 = riak_dt_vclock:merge([[{Actor, Cnt}], Clock]),
    InMap = sets:subtract(Adds, Removes),
    {CRDT0, ToRem} = lists:foldl(fun({F, Value, _X}=E, {CAcc, RAcc}) when F == Field ->
                                         {Type:merge(CAcc, Value), sets:add_element(E, RAcc)};
                                    (_, Acc) -> Acc
                                 end,
                                 {Type:new(), sets:new()},
                                 sets:to_list(InMap)),
    CRDT = Type:parent_clock(Clock2, CRDT0),
    case Type:update(Op, {Actor, Cnt}, CRDT, Ctx) of
        {ok, Updated} ->
            Model2 = Model#model{adds=sets:add_element({Field, Updated, Cnt}, Adds),
                                 removes=sets:union(ToRem, Removes),
                                 clock=Clock2},
            {ok, Model2};
        _ ->
            {ok, Model}
    end.

model_remove_field(Field, Model) ->
    #model{adds=Adds, removes=Removes} = Model,
    ToRemove = [{F, Val, Token} || {F, Val, Token} <- sets:to_list(Adds), F == Field],
    Model#model{removes=sets:union(Removes, sets:from_list(ToRemove))}.

model_merge(Model1, Model2) ->
    #model{adds=Adds1, removes=Removes1, deferred=Deferred1, clock=Clock1} = Model1,
    #model{adds=Adds2, removes=Removes2, deferred=Deferred2, clock=Clock2} = Model2,
    Clock = riak_dt_vclock:merge([Clock1, Clock2]),
    Adds0 = sets:union(Adds1, Adds2),
    %% @TODO model the merge with removed dots Any field that is only
    %% in removes must have a parent clock merge
    Removes0 = sets:union(Removes1, Removes2),
    Deferred0 = sets:union(Deferred1, Deferred2),
    {Adds, Removes, Deferred} = model_apply_deferred(Adds0, Removes0, Deferred0),
    #model{adds=Adds, removes=Removes, deferred=Deferred, clock=Clock}.

model_apply_deferred(Adds, Removes, Deferred) ->
    D2 = sets:subtract(Deferred, Adds),
    ToRem = sets:subtract(Deferred, D2),
    {Adds, sets:union(ToRem, Removes), D2}.

model_ctx_remove(Field, From, To) ->
    %% get adds for Field, any adds for field in ToAdds that are in
    %% FromAdds should be removed any others, put in deferred
    #model{adds=FromAdds} = From,
    #model{adds=ToAdds, removes=ToRemoves, deferred=ToDeferred} = To,
    ToRemove = sets:filter(fun({F, _Val, _Token}) -> F == Field end, FromAdds),
    Defer = sets:subtract(ToRemove, ToAdds),
    Remove = sets:subtract(ToRemove, Defer),
    To#model{removes=sets:union(Remove, ToRemoves),
           deferred=sets:union(Defer, ToDeferred)}.

model_value(Model) ->
    #model{adds=Adds, removes=Removes} = Model,
    Remaining = sets:subtract(Adds, Removes),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res)].

model_ctx(#model{clock=Ctx}) ->
    Ctx.

-endif. % EQC
