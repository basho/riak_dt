-module(halp).

-compile(export_all).

-record(halp, {actors, frs, rv, mv, m}).

halp(L) ->
    Actors = lists:foldl(fun({Actor, _Map, _Model}, Acc) ->
                                 ordsets:add_element(Actor, Acc) end,
                         ordsets:new(),
                         L),
    FinalReplicaState = lists:foldl(fun(Actor, Acc) ->
                                            [lists:keyfind(Actor, 1, L) | Acc] end,
                                    [],
                                    ordsets:to_list(Actors)),
    ReplicaValues = lists:foldl(fun({Actor, Map, Model}, Acc) ->
                                        [{Actor,
                                          riak_dt_map:value(Map),
                                          map_eqc:model_value(Model)} | Acc] end,
                                [],
                                FinalReplicaState),
    Merged={MMap, MModel} = lists:foldl(fun({_Actor, Map, Model}, {MM, MMo}) ->
                                       {riak_dt_map:merge(Map, MM), map_eqc:model_merge(Model, MMo)}
                               end,
                               {riak_dt_map:new(), map_eqc:model_new()},
                               FinalReplicaState),
    MergedValues = {riak_dt_map:value(MMap), map_eqc:model_value(MModel)},
    #halp{actors=Actors, frs=FinalReplicaState, rv=ReplicaValues, mv=MergedValues, m=Merged}.

