%% -------------------------------------------------------------------
%%
%% map_eqc: Drive out the merge bugs the other statem couldn't
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

-module(map2_eqc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{replicas=[],
               %% a unique tag per add
               counter=1 :: pos_integer(),
               %% fields that have been added
               adds=[] :: [{atom(), module()}]
              }).

-define(MAP, riak_dt_map2).
-define(MODEL, riak_dt_map).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).
%% @doc eunit runner
eqc_test_() ->
    {timeout, 200, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(100, ?QC_OUT(prop_merge()))))}.

%% @doc eunit runner for bin roundtrip
bin_roundtrip_test_() ->
    {timeout, 100, ?_assertEqual(true, eqc:quickcheck(eqc:testing_time(50, ?QC_OUT(crdt_statem_eqc:prop_bin_roundtrip(riak_dt_map)))))}.

%% @doc shell convenience, run eqc property 1000 times.
run() ->
    run(?NUMTESTS).

%% @doc shell convenience, run eqc property `Count' times
run(Count) ->
    eqc:quickcheck(eqc:numtests(Count, prop_merge())).

%% @doc shell convenience, check eqc property (last failing counter example)
check() ->
    eqc:check(prop_merge()).

%% Initialize the state
-spec initial_state() -> eqc_statem:symbolic_state().
initial_state() ->
    #state{}.

%% ------ Grouped operator: create_replica
create_replica_pre(#state{replicas=Replicas}) ->
    length(Replicas) < 10.

%% @doc create_replica_arge - Generate a replica
create_replica_args(_S) ->
    %% don't waste time shrinking actor id binaries
    [noshrink(binary(8))].

%% @doc create_replica_pre - Don't duplicate replicas
-spec create_replica_pre(S :: eqc_statem:symbolic_state(),
                         Args :: [term()]) -> boolean().
create_replica_pre(#state{replicas=Replicas}, [Id]) ->
    not lists:member(Id, Replicas).

%% @doc store new replica ID and bottom map/model in ets
create_replica(Id) ->
    ets:insert(map_eqc, {Id, ?MAP:new(), ?MODEL:new()}).

%% @doc create_replica_next - Add replica ID to state
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.


%% ------ Grouped operator: remove
%% @doc remove, but only something that has been added already
remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

%% @doc remove something that has been added already from any replica
remove_args(#state{adds=Adds, replicas=Replicas}) ->
    [
     elements(Replicas),
     elements(Adds) %% A Field that has been added
    ].

%% @doc correct shrinking
remove_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

%% @doc perform a remove operation, remove `Field' from Map/Model at
%% `Replica'
remove(Replica, Field) ->
    [{Replica, Map, Model}] = ets:lookup(map_eqc, Replica),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% Field being removed already, so ignore precon errors (they
    %% don't change state)
    {ok, Map2} = ignore_precon_error(?MAP:update({update, [{remove, Field}]}, Replica, Map), Map),
    {ok, Model2} = ignore_precon_error(?MODEL:update({update, [{remove, model_normalise_field(Field)}]}, Replica, Model), Model),
    ets:insert(map_eqc, {Replica, Map2, Model2}),
    {Map2, Model2}.

%% @doc remove post condition, @see post_all/2
remove_post(_S, [_Replica, Field], Res) ->
    post_all(Res, remove) andalso field_not_present(Field, Res).

%% ------ Grouped operator: ctx_remove
%% @doc remove, but with a context
ctx_remove_pre(#state{replicas=Replicas, adds=Adds}) ->
        Replicas /= [] andalso Adds /= [].

%% @doc generate ctx rmeove args
ctx_remove_args(#state{replicas=Replicas, adds=Adds}) ->
    [
     elements(Replicas),        %% read from
     elements(Replicas),          %% send op to
     elements(Adds)       %% which field to remove
    ].

%% @doc ensure correct shrinking
ctx_remove_pre(#state{replicas=Replicas, adds=Adds}, [From, To, Field]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas)
        andalso lists:member(Field, Adds).

%% @doc dynamic precondition, only context remove if the `Field' is in
%% the `From' replicas
ctx_remove_dynamicpre(_S, [From, _To, Field]) ->
    [{From, Map, _Model}] = ets:lookup(map_eqc, From),
    lists:keymember(Field, 1, ?MAP:value(Map)).

%% @doc perform a context remove on the map and model using context
%% from `From'
ctx_remove(From, To, Field) ->
    [{From, FromMap, FromModel}] = ets:lookup(map_eqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(map_eqc, To),
    Ctx = ?MAP:precondition_context(FromMap),
    {ok, Map} = ?MAP:update({update, [{remove, Field}]}, To, ToMap, Ctx),
    ModelCtx = ?MODEL:precondition_context(FromModel),
    {ok, Model} = ?MODEL:update({update, [{remove, model_normalise_field(Field)}]}, To, ToModel, ModelCtx),
    ets:insert(map_eqc, {To, Map, Model}),
    {Map, Model}.

%% @doc @see post_all/2
ctx_remove_post(_S, _Args, Res) ->
    post_all(Res, ctx_remove).

%% ------ Grouped operator: replicate Merges two replicas' values.

%% @doc must be a replica at least.
replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc chose from/to replicas, can be the same
replicate_args(#state{replicas=Replicas}) ->
    [
     elements(Replicas), %% Replicate from
     elements(Replicas) %% Replicate to
    ].

%% @doc replicate_pre - shrink correctly
-spec replicate_pre(S :: eqc_statem:symbolic_state(),
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc Replicate a CRDT from `From' to `To'
replicate(From, To) ->
    [{From, FromMap, FromModel}] = ets:lookup(map_eqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(map_eqc, To),

    Map = ?MAP:merge(FromMap, ToMap),
    Model = ?MODEL:merge(FromModel, ToModel),

    ets:insert(map_eqc, {To, Map, Model}),
    {Map, Model}.

%% @doc @see post_all/2
replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% ------ Grouped operator: ctx_update
%% Update a Field in the Map, using the Map context

%% @doc there must be at least one replica
ctx_update_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc generate an operation
ctx_update_args(#state{replicas=Replicas, counter=Cnt}) ->
    ?LET({Field, Op}, gen_field_and_op(),
         [
          Field,
          Op,
          elements(Replicas),
          elements(Replicas),
          Cnt
         ]).

%% @doc ensure correct shrinking
ctx_update_pre(#state{replicas=Replicas}, [_Field, _Op, From, To, _Cnt]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

%% @doc much like context_remove, get a contet from `From' and apply
%% `Op' at `To'
ctx_update(Field, Op, From, To, _Cnt) ->
    [{From, CtxMap, CtxModel}] = ets:lookup(map_eqc, From),
    [{To, ToMap, ToModel}] = ets:lookup(map_eqc, To),

    Ctx = ?MAP:precondition_context(CtxMap),
    ModCtx = ?MODEL:precondition_context(CtxModel),
    {ok, Map} = ?MAP:update({update, [{update, Field, Op}]}, To, ToMap, Ctx),
    {ok, Model} = ?MODEL:update({update, [{update, model_normalise_field(Field), Op}]}, To, ToModel, ModCtx),

    ets:insert(map_eqc, {To, Map, Model}),
    {Map, Model}.

%% @doc update the model state, incrementing the counter represents logical time.
ctx_update_next(S=#state{counter=Cnt, adds=Adds}, _Res, [Field, _Op, _From, _To, _Cnt]) ->
    S#state{adds=lists:umerge(Adds, [Field]), counter=Cnt+1}.

%% @doc @see post_all/2
ctx_update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% ------ Grouped operator: update
%% Update a Field in the Map

%% @doc there must be at least one replica
update_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc choose a field, operation and replica
update_args(#state{replicas=Replicas, counter=Cnt}) ->
    ?LET({Field, Op}, gen_field_and_op(),
         [
          Field,
          Op,
          elements(Replicas),
          Cnt
         ]).

%% @doc shrink correctly
update_pre(#state{replicas=Replicas}, [_Field, _Op, Replica, _Cnt]) ->
    lists:member(Replica, Replicas).

%% @doc apply `Op' to `Field' at `Replica'
update(Field, Op, Replica, _Cnt) ->
    [{Replica, Map0, Model0}] = ets:lookup(map_eqc, Replica),

    {ok, Map} = ignore_precon_error(?MAP:update({update, [{update, Field, Op}]}, Replica, Map0), Map0),
    ModelField = model_normalise_field(Field),
    {ok, Model} = ignore_precon_error(?MODEL:update({update, [{update, ModelField, Op}]}, Replica, Model0), Model0),

    ets:insert(map_eqc, {Replica, Map, Model}),
    {Map, Model}.

%% @doc increment the time counter, and add this field to adds as a
%% candidate to be removed later.
update_next(S=#state{counter=Cnt, adds=Adds}, _Res, [Field, _, _, _]) ->
    S#state{adds=lists:umerge(Adds, [Field]), counter=Cnt+1}.

%% @doc @see post_all/2
update_post(_S, _Args, Res) ->
    post_all(Res, update).

%% ------ Grouped operator: idempotent

idempotent_args(#state{replicas=Replicas}) ->
    [elements(Replicas)].

%% @doc idempotent_pre - Precondition for generation
-spec idempotent_pre(S :: eqc_statem:symbolic_state()) -> boolean().
idempotent_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc idempotent_pre - Precondition for idempotent
-spec idempotent_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
idempotent_pre(#state{replicas=Replicas}, [Replica]) ->
    lists:member(Replica, Replicas).

%% @doc idempotent - Merge replica with itself, result used for post condition only
idempotent(Replica) ->
    [{Replica, Map, _Model}] = ets:lookup(map_eqc, Replica),
    {Map, ?MAP:merge(Map, Map)}.

%% @doc idempotent_post - Postcondition for idempotent
-spec idempotent_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
idempotent_post(_S, [Replica], {Map, MergedSelfMap}) ->
    case ?MAP:equal(Map, MergedSelfMap) of
        true ->
            true;
        _Wut ->
            {postcondition_failed, {Replica, Map, MergedSelfMap}}
    end.

%% ------ Grouped operator: commutative

commutative_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas)].

%% @doc commutative_pre - Precondition for generation
-spec commutative_pre(S :: eqc_statem:symbolic_state()) -> boolean().
commutative_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc commutative_pre - Precondition for commutative
-spec commutative_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
commutative_pre(#state{replicas=Replicas}, [Replica, Replica2]) ->
    lists:member(Replica, Replicas) andalso lists:member(Replica2, Replicas).

%% @doc commutative - Merge maps both ways (result used for post condition)
commutative(Replica1, Replica2) ->
    [{Replica1, Map1, _Model1}] = ets:lookup(map_eqc, Replica1),
    [{Replica2, Map2, _Model2}] = ets:lookup(map_eqc, Replica2),
    {?MAP:merge(Map1, Map2), ?MAP:merge(Map2, Map1)}.

%% @doc commutative_post - Postcondition for commutative
-spec commutative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
commutative_post(_S, [_Replica1, _Replica2], {OneMergeTwo, TwoMergeOne}) ->
    ?MAP:equal(OneMergeTwo, TwoMergeOne).

%% ------ Grouped operator: associative

associative_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas), elements(Replicas)].

%% @doc associative_pre - Precondition for generation
-spec associative_pre(S :: eqc_statem:symbolic_state()) -> boolean().
associative_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc associative_pre - Precondition for associative
-spec associative_pre(S :: eqc_statem:symbolic_state(),
                     Args :: [term()]) -> boolean().
associative_pre(#state{replicas=Replicas}, [Replica, Replica2, Replica3]) ->
    lists:member(Replica, Replicas)
        andalso lists:member(Replica2, Replicas)
        andalso lists:member(Replica3, Replicas).

%% @doc associative - Merge maps three ways (result used for post condition)
associative(Replica1, Replica2, Replica3) ->
    [{Replica1, Map1, _Model1}] = ets:lookup(map_eqc, Replica1),
    [{Replica2, Map2, _Model2}] = ets:lookup(map_eqc, Replica2),
    [{Replica3, Map3, _Model3}] = ets:lookup(map_eqc, Replica3),
    {?MAP:merge(?MAP:merge(Map1, Map2), Map3),
     ?MAP:merge(?MAP:merge(Map1, Map3), Map2),
     ?MAP:merge(?MAP:merge(Map2, Map3), Map1)}.

%% @doc associative_post - Postcondition for associative
-spec associative_post(S :: eqc_statem:dynamic_state(),
                      Args :: [term()], R :: term()) -> true | term().
associative_post(_S, [_Replica1, _Replica2, _Replica3], {ABC, ACB, BCA}) ->
%%    case {riak_dt_map2:equal(ABC, ACB),  riak_dt_map2:equal(ACB, BCA)} of
    case {map_values_equal(ABC, ACB), map_values_equal(ACB, BCA)} of
        {true, true} ->
            true;
        {false, true} ->
            {postcondition_failed, {ACB, not_associative, ABC}};
        {true, false} ->
            {postcondition_failed, {ACB, not_associative, BCA}};
        {false, false} ->
            {postcondition_failed, {{ACB, not_associative, ABC}, '&&', {ACB, not_associative, BCA}}}
    end.

map_values_equal(Map1, Map2) ->
    lists:sort(?MAP:value(Map1)) == lists:sort(?MAP:value(Map2)).

%% @Doc Weights for commands. Don't create too many replicas, but
%% prejudice in favour of creating more than 1. Try and balance
%% removes with adds. But favour adds so we have something to
%% remove. See the aggregation output.
weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(S, create_replica) when length(S#state.replicas) < 5 ->
    4;
weight(_S, remove) ->
    2;
weight(_S, context_remove) ->
    5;
weight(_S, update) ->
    3;
weight(_S, ctx_update) ->
    3;
weight(_S, _) ->
    1.


%% @doc Tests the property that a riak_dt_map2 is equivalent to the Map
%% Model. The Map Model is based roughly on an or-set design. Inspired
%% by a draft spec in sent in private email by Carlos Baquero. The
%% model extends the spec to onclude context operations, deferred
%% operations, and reset-remove semantic (with tombstones.)
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                %% store state in eqc, external to statem state
                ets:new(map_eqc, [named_table, set]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                ReplicaData =  ets:tab2list(map_eqc),
                %% Check that merging all values leads to the same results for Map and the Model
                {Map, Model} = lists:foldl(fun({_Actor, InMap, InModel}, {M, Mo}) ->
                                                   {?MAP:merge(M, InMap),
                                                    ?MODEL:merge(Mo, InModel)}
                                           end,
                                           {?MAP:new(), ?MODEL:new()},
                                           ReplicaData),
                MapValue = ?MAP:value(Map),
                ModelValue = model_value(Model),
                %% clean up
                ets:delete(map_eqc),
                %% prop
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                measure(actors, length(ReplicaData),
                                        measure(length, length(MapValue),
                                                measure(depth, map_depth(MapValue),
                                                        aggregate(command_names(Cmds),
                                                                  conjunction([{results, equals(Res, ok)},
                                                                               {value, equals(lists:sort(MapValue), lists:sort(ModelValue))}
                                                                              ])
                                                                 )))))
            end).

%% -----------
%% Generators
%% ----------

%% @doc Keep the number of possible field names down to a minimum. The
%% smaller state space makes EQC more likely to find bugs since there
%% will be more action on the fields. Learned this from
%% crdt_statem_eqc having to large a state space and missing bugs.
gen_field() ->
    {growingelements(['A', 'B', 'C', 'D', 'X', 'Y', 'Z']),
     elements([
               riak_dt_orswot,
               riak_dt_emcntr,
               riak_dt_emlwwreg,
               ?MAP,
               riak_dt_od_flag
              ])}.

%% @use the generated field to generate an op. Delegates to type. Some
%% Type generators are recursive, pass in Size to limit the depth of
%% recusrions. @see riak_dt_map2:gen_op/1.
gen_field_op({_Name, Type}) ->
    ?SIZED(Size, Type:gen_op(Size)).

%% @doc geneate a field, and a valid operation for the field
gen_field_and_op() ->
    ?LET(Field, gen_field(), {Field, gen_field_op(Field)}).


%% -----------
%% Helpers
%% ----------

%% @doc how deeply nested is `Map'? Recurse down the Map and return
%% the deepest nesting. A depth of `1' means only a top-level Map. `2'
%% means a map in the seocnd level, `3' in the third, and so on.
map_depth(Map) ->
    map_depth(Map, 1).

%% @doc iterate a maps fields, and recurse down map fields to get a
%% max depth.
map_depth(Map, D) ->
    lists:foldl(fun({{_, riak_dt_map2}, SubMap}, MaxDepth) ->
                        Depth = map_depth(SubMap, D+1),
                        max(MaxDepth, Depth);
                   (_, Depth) ->
                        Depth
                end,
                D,
                Map).

%% @doc precondition errors don't change the state of a map, so ignore
%% them.
ignore_precon_error({ok, NewMap}, _) ->
    {ok, NewMap};
ignore_precon_error(_, Map) ->
    {ok, Map}.

%% @doc for all mutating operations enusre that the state at the
%% mutated replica is equal for the map and the model
post_all({Map, Model}, Cmd) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(?MAP:value(Map)) == lists:sort(model_value(Model)) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Map and Model don't match", Cmd, Map, Model, ?MAP:value(Map), model_value(Model)}
    end.

%% @doc `true' if `Field' is not in `Map'
field_not_present(Field, {Map, _Model}) ->
    case lists:keymember(Field, 1, ?MAP:value(Map)) of
        false ->
            true;
        true ->
            {Field, present, Map}
    end.

%% The ?MAP uses the embedded emlwwreg to match the semantics of the
%% old map (?MODEL) and its lwwreg
model_normalise_field({Name, riak_dt_emlwwreg}) ->
    {Name, riak_dt_lwwreg};
model_normalise_field({Name, ?MAP}) ->
    {Name, ?MODEL};
model_normalise_field(F) ->
    F.

model_denormalise_field({Name, riak_dt_lwwreg}) ->
    {Name, riak_dt_emlwwreg};
model_denormalise_field({Name, ?MODEL}) ->
    {Name, ?MAP};
model_denormalise_field(F) ->
    F.

%% recurse down value and replace Field with
%% model_denormalise_field(Field)
model_value(Model) ->
    Val0 = ?MODEL:value(Model),
    normalise_value(?MODEL, Val0).

normalise_value(?MODEL, Pairs) ->
    [begin
         {_N, T}=F = model_denormalise_field(Field),
         {F, normalise_value(T, Val)}
     end || {Field, Val} <- Pairs];
normalise_value(_T, Val) ->
    Val.

-endif. % EQC
