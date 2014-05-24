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

-module(map_eqc2).

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
          %% For reset-remove semantic, the field+clock at time of
          %% removal
          tombstones=orddict:new() :: orddict:orddict(),
          %% for embedded context operations
          clock=riak_dt_vclock:fresh() :: riak_dt_vclock:vclock()
         }).

%%-type map_model() :: #model{}.

-record(state,{replicas=[],
               %% a unique tag per add
               counter=1 :: pos_integer(),
               %% things that have been added
               adds=[] :: [{ActorId :: binary(), atom()}]
              }).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 200, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(100, ?QC_OUT(prop_merge()))))}.

bin_roundtrip_test_() ->
    {timeout, 100, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(crdt_statem_eqc:prop_bin_roundtrip(riak_dt_map)))))}.


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

%% ------ Grouped operator: create_replica
create_replica_pre(#state{replicas=Replicas}) ->
    length(Replicas) < 10.

%% @doc create_replica_command - Command generator
create_replica_args(_S) ->
    [noshrink(binary(8))].

%% @doc create_replica_pre - Precondition for create_replica
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

create_replica(Id) ->
    ets:insert(crdteqc, {Id, riak_dt_map:new(), model_new()}).

%% @doc create_replica_next - Next state function
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.


%% ------ Grouped operator: remove

%% remove, but only something that has been added already
remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

remove_args(#state{adds=Adds, replicas=Replicas}) ->
    [
     elements(Replicas),
     elements(Adds) %% A Field that has been added
    ].

remove_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

remove(Replica, Field) ->
    [{Replica, Map, Model}] = ets:lookup(crdteqc, Replica),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% Field being removed already, so ignore precon errors (they
    %% don't change state)
    {ok, Map2} = ignore_precon_error(riak_dt_map:update({update, [{remove, Field}]}, Replica, Map), Map),
    Model2 = model_remove_field(Field, Model),
    ets:insert(crdteqc, {Replica, Map2, Model2}),
    {Map2, Model2}.

remove_post(_S, _Args, Res) ->
    post_all(Res, remove).

%% ------ Grouped operator: ctx_remove
%% remove, but with a context
ctx_remove_pre(#state{replicas=Replicas, adds=Adds}) ->
        Replicas /= [] andalso Adds /= [].

ctx_remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [
     elements(Replicas),        %% read from
     elements(Replicas),          %% send op to
     elements(Adds)       %% which field to remove
    ].

ctx_remove_pre(#state{replicas=Replicas, adds=Adds}, [From, To, Field]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas)
        andalso lists:member(Field, Adds).

ctx_remove_dynamicpre(_S, [From, _To, Field]) ->
    [{From, Map, _Model}] = ets:lookup(crdteqc, From),
    lists:keymember(Field, 1, riak_dt_map:value(Map)).

ctx_remove(From, To, Field) ->
    [{From, FromMap, FromModel}] = ets:lookup(crdteqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(crdteqc, To),
    Ctx = riak_dt_map:precondition_context(FromMap),
    {ok, Map} = riak_dt_map:update({update, [{remove, Field}]}, To, ToMap, Ctx),
    Model = model_ctx_remove(Field, FromModel, ToModel),
    ets:insert(crdteqc, {To, Map, Model}),
    {Map, Model}.

ctx_remove_post(_S, _Args, Res) ->
    post_all(Res, ctx_remove).

%% ------ Grouped operator: replicate
%% Merge two replicas' values
replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

replicate_args(#state{replicas=Replicas}) ->
    [
     elements(Replicas), %% Replicate from
     elements(Replicas) %% Replicate to
    ].

%% @doc replicate_pre - Precondition for replicate
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% Replicate a CRDT from `From' to `To'
replicate(From, To) ->
    [{From, FromMap, FromModel}] = ets:lookup(crdteqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(crdteqc, To),

    Map = riak_dt_map:merge(FromMap, ToMap),
    Model = model_merge(FromModel, ToModel),

    ets:insert(crdteqc, {To, Map, Model}),
    {Map, Model}.

replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% ------ Grouped operator: ctx_update
%% Update a Field in the Map, using the Map context
ctx_update_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

ctx_update_args(#state{replicas=Replicas, counter=Cnt}) ->
    ?LET({Field, Op}, gen_field_and_op(),
         [
          Field,
          Op,
          elements(Replicas),
          elements(Replicas),
          Cnt
         ]).

ctx_update_pre(#state{replicas=Replicas}, [_Field, _Op, From, To, _Cnt]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

ctx_update(Field, Op, From, To, Cnt) ->
    [{From, CtxMap, CtxModel}] = ets:lookup(crdteqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(crdteqc, To),

    Ctx = riak_dt_map:precondition_context(CtxMap),
    ModCtx = model_ctx(CtxModel),
    {ok, Map} = riak_dt_map:update({update, [{update, Field, Op}]}, To, ToMap, Ctx),
    {ok, Model} = model_update_field(Field, Op, To, Cnt, ToModel, ModCtx),

    ets:insert(crdteqc, {To, Map, Model}),
    {Map, Model}.

ctx_update_next(S=#state{counter=Cnt, adds=Adds}, _Res, [Field, _Op, _From, _To, _Cnt]) ->
    S#state{adds=lists:umerge(Adds, [Field]), counter=Cnt+1}.

ctx_update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% ------ Grouped operator: update
%% Update a Field in the Map
update_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

update_args(#state{replicas=Replicas, counter=Cnt}) ->
    ?LET({Field, Op}, gen_field_and_op(),
         [
          Field,
          Op,
          elements(Replicas),
          Cnt
         ]).

update_pre(#state{replicas=Replicas}, [_Field, _Op, Replica, _Cnt]) ->
    lists:member(Replica, Replicas).

update(Field, Op, Replica, Cnt) ->
    [{Replica, Map0, Model0}] = ets:lookup(crdteqc, Replica),

    {ok, Map} = ignore_precon_error(riak_dt_map:update({update, [{update, Field, Op}]}, Replica, Map0), Map0),
    {ok, Model} = model_update_field(Field, Op, Replica, Cnt, Model0,  undefined),

    ets:insert(crdteqc, {Replica, Map, Model}),
    {Map, Model}.

update_next(S=#state{counter=Cnt, adds=Adds}, _Res, [Field, _, _, _]) ->
    S#state{adds=lists:umerge(Adds, [Field]), counter=Cnt+1}.

update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% Tests the property that an Map is equivalent to the Map Model
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                ets:new(crdteqc, [named_table, set]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                ReplicaData =  ets:tab2list(crdteqc),
                %% Check that collapsing all values leads to the same results for Map and the Model
                {Map, Model} = lists:foldl(fun({_Actor, InMap, InModel}, {M, Mo}) ->
                                                   {riak_dt_map:merge(M, InMap),
                                                    model_merge(Mo, InModel)}
                                           end,
                                           {riak_dt_map:new(), model_new()},
                                           ReplicaData),
                MapValue = riak_dt_map:value(Map),
                ModelValue = model_value(Model),
                ets:delete(crdteqc),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                measure(actors, length(ReplicaData),
                                        measure(length, length(MapValue),
                                                measure(depth, map_depth(MapValue),
                                                        aggregate(command_names(Cmds),
                                                                  conjunction([{results, equals(Res, ok)},
                                                                               {value, equals(lists:sort(MapValue), lists:sort(ModelValue))}%%,
                                                                               %% {actors, sets:is_subset(sets:from_list(Actors), sets:from_list(Replicas))}
                                                                              ])
                                                                 )))))
            end).

%% -----------
%% Generators
%% ----------
%% Keep the number of possible field names down to a minimum. The
%% smaller state space makes EQC more likely to find bugs since there
%% will be more action on the fields. Learned this from
%% crdt_statem_eqc having to large a state space and missing bugs.
gen_field() ->
    {growingelements(['A', 'B', 'C', 'D', 'X', 'Y', 'Z']),
     elements([
            riak_dt_orswot,
            riak_dt_emcntr,
            riak_dt_lwwreg,
            riak_dt_map,
            riak_dt_od_flag
           ])}.

gen_field_op({_Name, Type}) ->
    ?SIZED(Size, Type:gen_op(Size)).

gen_field_and_op() ->
    ?LET(Field, gen_field(), {Field, gen_field_op(Field)}).


%% -----------
%% Helpers
%% ----------
map_depth(Map) ->
    map_depth(Map, 1).

map_depth(Map, D) ->
    lists:foldl(fun({{_, riak_dt_map}, SubMap}, MaxDepth) ->
                        Depth = map_depth(SubMap, D+1),
                        max(MaxDepth, Depth);
                   (_, Depth) ->
                        Depth
                end,
                D,
                Map).

%% precondition errors don't change the state of a map
ignore_precon_error({ok, NewMap}, _) ->
    {ok, NewMap};
ignore_precon_error(_, Map) ->
    {ok, Map}.

post_all({Map, Model}, Cmd) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(riak_dt_map:value(Map)) == lists:sort(model_value(Model)) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Map and Model don't match", Cmd, Map, Model, riak_dt_map:value(Map), model_value(Model)}
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
    #model{adds=Adds, removes=Removes, clock=Clock, tombstones=TS} = Model,
    Clock2 = riak_dt_vclock:merge([[{Actor, Cnt}], Clock]),
    InMap = sets:subtract(Adds, Removes),
    {CRDT0, ToRem} = lists:foldl(fun({F, Value, _X}=E, {CAcc, RAcc}) when F == Field ->
                                         {Type:merge(CAcc, Value), sets:add_element(E, RAcc)};
                                    (_, Acc) -> Acc
                                 end,
                                 {Type:new(), sets:new()},
                                 sets:to_list(InMap)),
    CRDT1 = case orddict:find(Field, TS) of
                error ->
                    CRDT0;
                {ok, TSVal} ->
                    Type:merge(CRDT0, TSVal)
            end,

    CRDT = Type:parent_clock(Clock2, CRDT1),
    case Type:update(Op, {Actor, Cnt}, CRDT, Ctx) of
        {ok, Updated} ->
            Model2 = Model#model{adds=sets:add_element({Field, Updated, Cnt}, Adds),
                                 removes=sets:union(ToRem, Removes),
                                 clock=Clock2},
            {ok, Model2};
        _ ->
            {ok, Model}
    end.

model_remove_field({_Name, Type}=Field, Model) ->
    #model{adds=Adds, removes=Removes, tombstones=Tombstones0, clock=Clock} = Model,
    ToRemove = [{F, Val, Token} || {F, Val, Token} <- sets:to_list(Adds), F == Field],
    TS = Type:parent_clock(Clock, Type:new()),
    Tombstones = orddict:update(Field, fun(T) -> Type:merge(TS, T) end, TS, Tombstones0),
    Model#model{removes=sets:union(Removes, sets:from_list(ToRemove)), tombstones=Tombstones}.

model_merge(Model1, Model2) ->
    #model{adds=Adds1, removes=Removes1, deferred=Deferred1, clock=Clock1, tombstones=TS1} = Model1,
    #model{adds=Adds2, removes=Removes2, deferred=Deferred2, clock=Clock2, tombstones=TS2} = Model2,
    Clock = riak_dt_vclock:merge([Clock1, Clock2]),
    Adds0 = sets:union(Adds1, Adds2),
    Tombstones = orddict:merge(fun({_Name, Type}, V1, V2) -> Type:merge(V1, V2) end, TS1, TS2),
    Removes0 = sets:union(Removes1, Removes2),
    Deferred0 = sets:union(Deferred1, Deferred2),
    {Adds, Removes, Deferred} = model_apply_deferred(Adds0, Removes0, Deferred0),
    #model{adds=Adds, removes=Removes, deferred=Deferred, clock=Clock, tombstones=Tombstones}.

model_apply_deferred(Adds, Removes, Deferred) ->
    D2 = sets:subtract(Deferred, Adds),
    ToRem = sets:subtract(Deferred, D2),
    {Adds, sets:union(ToRem, Removes), D2}.

model_ctx_remove({_N, Type}=Field, From, To) ->
    %% get adds for Field, any adds for field in ToAdds that are in
    %% FromAdds should be removed any others, put in deferred
    #model{adds=FromAdds, clock=FromClock} = From,
    #model{adds=ToAdds, removes=ToRemoves, deferred=ToDeferred, tombstones=TS} = To,
    ToRemove = sets:filter(fun({F, _Val, _Token}) -> F == Field end, FromAdds),
    Defer = sets:subtract(ToRemove, ToAdds),
    Remove = sets:subtract(ToRemove, Defer),
    Tombstone = Type:parent_clock(FromClock, Type:new()),
    TS2 = orddict:update(Field, fun(T) -> Type:merge(T, Tombstone) end, Tombstone, TS),
    To#model{removes=sets:union(Remove, ToRemoves),
           deferred=sets:union(Defer, ToDeferred),
            tombstones=TS2}.

model_value(Model) ->
    #model{adds=Adds, removes=Removes, tombstones=TS} = Model,
    Remaining = sets:subtract(Adds, Removes),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                              %% if key is in Acc merge with it and replace
                              dict:update(Key, fun(V) ->
                                                       Type:merge(V, Value) end,
                                          Value, Acc) end,
                      dict:new(),
                      sets:to_list(Remaining)),
    Res2 = dict:fold(fun({_N, Type}=Field, Val, Acc) ->
                             case orddict:find(Field, TS) of
                                 error ->
                                     dict:store(Field, Val, Acc);
                                 {ok, TSVal} ->
                                     dict:store(Field, Type:merge(TSVal, Val), Acc)
                             end
                     end,
                     dict:new(),
                     Res),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res2)].

model_ctx(#model{clock=Ctx}) ->
    Ctx.


-endif. % EQC
