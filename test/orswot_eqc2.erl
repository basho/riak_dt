%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 22 May 2014 by Russell Brown <russelldb@basho.com>

-module(orswot_eqc2).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state, {replicas=[], adds=[], contents=[]}).

-define(ELEMENTS, ['A', 'B', 'C', 'D', 'X', 'Y', 'Z']).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

eqc_test_() ->
    {timeout, 60, ?_assertEqual(true, eqc:quickcheck(eqc:numtests(1000, ?QC_OUT(prop_crdt()))))}.

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
    ets:insert(crdteqc, {Id, riak_dt_orswot:new(), {riak_dt_orset:new(), riak_dt_orset:new()}}).

%% @doc create_replica_next - Next state function
-spec create_replica_next(S :: eqc_statem:symbolic_state(),
                          V :: eqc_statem:var(),
                          Args :: [term()]) -> eqc_statem:symbolic_state().
create_replica_next(S=#state{replicas=R0}, _Value, [Id]) ->
    S#state{replicas=R0++[Id]}.

%% ------ Grouped operator: add
add_args(#state{replicas=Replicas}) ->
    [elements(Replicas),
     growingelements(?ELEMENTS)].

%% @doc add_pre - Precondition for generation
-spec add_pre(S :: eqc_statem:symbolic_state()) -> boolean().
add_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc add_pre - Precondition for add
-spec add_pre(S :: eqc_statem:symbolic_state(),
              Args :: [term()]) -> boolean().
add_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

add(Replica, Element) ->
    [{Replica, ORSWOT, {ORSet, Def}}] = ets:lookup(crdteqc, Replica),
    {ok, ORSWOT2} = riak_dt_orswot:update({add, Element}, Replica, ORSWOT),
    {ok, ORSet2} = riak_dt_orset:update({add, Element}, Replica, ORSet),
    ets:insert(crdteqc, {Replica, ORSWOT2, {ORSet2, Def}}),
    {ORSWOT2, ORSet2}.

%% @doc add_next - Next state function
-spec add_next(S :: eqc_statem:symbolic_state(), 
               V :: eqc_statem:var(), 
               Args :: [term()]) -> eqc_statem:symbolic_state().
add_next(S=#state{adds=Adds, contents=Contents}, _Value, [_, Element]) ->
    S#state{adds=lists:umerge(Adds, [Element]),
            contents=lists:umerge(Contents, [Element])}.

%% @doc add_post - Postcondition for add
-spec add_post(S :: eqc_statem:dynamic_state(), 
               Args :: [term()], R :: term()) -> true | term().
add_post(_S, _Args, {ORSWOT, ORSet}) ->
    sets_equal(ORSWOT, ORSet).

%% ------ Grouped operator: remove
%% @doc remove_command - Command generator

%% @doc remove_pre - Precondition for generation
-spec remove_pre(S :: eqc_statem:symbolic_state()) -> boolean().
remove_pre(#state{replicas=Replicas, adds=Adds}) ->
    Replicas /= [] andalso Adds /= [].

remove_args(#state{replicas=Replicas, adds=Adds}) -> 
    [elements(Replicas), elements(Adds)].

remove_pre(#state{replicas=Replicas}, [Replica, _]) ->
    lists:member(Replica, Replicas).

remove(Replica, Value) ->
    [{Replica, ORSWOT, {ORSet, Def}}] = ets:lookup(crdteqc, Replica),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% element being removed already, so ignore precon errors (they
    %% don't change state)
    ORSWOT2 = ignore_preconerror_remove(Value, Replica, ORSWOT, riak_dt_orswot),
    ORSet2 = ignore_preconerror_remove(Value, Replica, ORSet, riak_dt_orset),
    ets:insert(crdteqc, {Replica, ORSWOT2, {ORSet2, Def}}),
    {ORSWOT2, ORSet2}.

remove_post(_S, [_, Element], {ORSWOT2, ORSet2}) ->
    eqc_statem:conj([sets_not_member(Element, ORSWOT2),
          sets_equal(ORSWOT2, ORSet2)]).

%% @doc remove_next - Next state function
-spec remove_next(S :: eqc_statem:symbolic_state(), 
                  V :: eqc_statem:var(), 
                  Args :: [term()]) -> eqc_statem:symbolic_state().
remove_next(S=#state{contents=Contents}, _Value, [_, Element]) ->
    S#state{contents=lists:delete(Element, Contents)}.

%% ------ Grouped operator: replicate
%% @doc replicate_args - Command generator
replicate_args(#state{replicas=Replicas}) ->
    [elements(Replicas), elements(Replicas)].

%% @doc replicate_pre - Precondition for generation
-spec replicate_pre(S :: eqc_statem:symbolic_state()) -> boolean().
replicate_pre(#state{replicas=Replicas}) ->
    Replicas /= [].

%% @doc replicate_pre - Precondition for replicate
-spec replicate_pre(S :: eqc_statem:symbolic_state(), 
                    Args :: [term()]) -> boolean().
replicate_pre(#state{replicas=Replicas}, [From, To]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas).

replicate(From, To) ->
    [{From, FromORSWOT, {FromORSet, FromDef}}] = ets:lookup(crdteqc, From),
    [{To, ToORSWOT, {ToORSet, ToDef}}] = ets:lookup(crdteqc, To),

    ORSWOT = riak_dt_orswot:merge(FromORSWOT, ToORSWOT),
    ORSet0 = riak_dt_orset:merge(FromORSet, ToORSet),
    Def0 = riak_dt_orset:merge(FromDef, ToDef),

    {ORSet, Def} = model_apply_deferred(ORSet0, Def0),

    ets:insert(crdteqc, {To, ORSWOT, {ORSet, Def}}),
    {ORSWOT, ORSet}.

%% @doc replicate_post - Postcondition for replicate
-spec replicate_post(S :: eqc_statem:dynamic_state(), 
                     Args :: [term()], R :: term()) -> true | term().
replicate_post(_S, [_From, _To], {SWOT, Set}) ->
    sets_equal(SWOT, Set).


%% ------ Grouped operator: context_remove
%% @doc context_remove_command - Command generator
context_remove_args(#state{replicas=Replicas, adds=Adds}) -> 
    [elements(Replicas),
     elements(Replicas),
     elements(Adds)].

%% @doc context_remove_pre - Precondition for generation
-spec context_remove_pre(S :: eqc_statem:symbolic_state()) -> boolean().
context_remove_pre(#state{replicas=Replicas, adds=Adds}) -> 
    Replicas /= [] andalso Adds /= [].

%% @doc context_remove_pre - Precondition for context_remove
-spec context_remove_pre(S :: eqc_statem:symbolic_state(), 
                         Args :: [term()]) -> boolean().
context_remove_pre(#state{replicas=Replicas, adds=Adds}, [From, To, Element]) ->
    lists:member(From, Replicas) andalso lists:member(To, Replicas)
        andalso lists:member(Element, Adds).

context_remove_dynamicpre(_S, [From, _To, Element]) ->
    [{From, SWOT, _FromORSet}] = ets:lookup(crdteqc, From),
    lists:member(Element, riak_dt_orswot:value(SWOT)).

context_remove(From, To, Element) ->
    [{From, FromORSWOT, {FromORSet, _FromDef}}] = ets:lookup(crdteqc, From),
    [{To, ToORSWOT, {ToORSet, ToDef}}] = ets:lookup(crdteqc, To),

    Ctx = riak_dt_orswot:precondition_context(FromORSWOT),
    Fragment = riak_dt_orset:value({fragment, Element}, FromORSet),
    {ok, ToORSWOT2} = riak_dt_orswot:update({remove, Element}, To, ToORSWOT, Ctx),

    {ToORSet2, ToDef2} = context_remove_model(Element, Fragment, ToORSet, ToDef),

    ets:insert(crdteqc, {To, ToORSWOT2, {ToORSet2, ToDef2}}),
    {ToORSWOT2, ToORSet2}.

%% @doc context_remove_next - Next state function
-spec context_remove_next(S :: eqc_statem:symbolic_state(), 
                          V :: eqc_statem:var(), 
                          Args :: [term()]) -> eqc_statem:symbolic_state().
context_remove_next(S=#state{contents=Contents}, _Value, [_From, _To, Element]) ->
    S#state{contents=lists:delete(Element, Contents)}.

weight(S, create_replica) when length(S#state.replicas) > 2 ->
    1;
weight(_S, remove) ->
    3;
weight(_S, context_remove) ->
    3;
weight(_S, add) ->
    8;
weight(_S, _) ->
    1.

%% @doc Default generated property
-spec prop_crdt() -> eqc:property().
prop_crdt() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                ets:new(crdteqc, [named_table, set]),
                {H, S, Res} = run_commands(?MODULE,Cmds),
                {MergedSwot, {MergedSet, _}} = lists:foldl(fun({_Id, ORSWOT, ORSetAndDef}, {MO, MOS}) ->
                                                              {riak_dt_orswot:merge(ORSWOT, MO),
                                                               model_merge(MOS, ORSetAndDef)}
                                                      end,
                                                      {riak_dt_orswot:new(), {riak_dt_orset:new(), riak_dt_orset:new()}},
                                                      ets:tab2list(crdteqc)),

                ets:delete(crdteqc),
                pretty_commands(?MODULE, Cmds, {H, S, Res},
                                aggregate(command_names(Cmds),
                                          collect(with_title(replicas), length(S#state.replicas),
                                                  collect(with_title(elements), riak_dt_orswot:stat(element_count, MergedSwot)
                                                         ,conjunction([{result, Res == ok},
                                                                       {equal, sets_equal(MergedSwot, MergedSet)}%% ,
                                                                      ])))))
            end).

%% Helpers
ignore_preconerror_remove(Value, Actor, Set, Mod) ->
    case Mod:update({remove, Value}, Actor, Set) of
        {ok, Set2} ->
            Set2;
        _E ->
            Set
    end.

sets_equal(ORSWOT, ORSet) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(riak_dt_orswot:value(ORSWOT)) ==
        lists:sort(riak_dt_orset:value(ORSet)) of
        true ->
            true;
        _ ->
            {ORSWOT, '/=', ORSet}
    end.

sets_not_member(Element, ORSWOT) ->
    %% What matters is that both types have the exact same results.
    case lists:member(Element, riak_dt_orswot:value(ORSWOT)) of
        false ->
            true;
        _ ->
            {Element, member, ORSWOT}
    end.

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

model_merge({S1, D1}, {S2, D2}) ->
    S = riak_dt_orset:merge(S1, S2),
    D = riak_dt_orset:merge(D1, D2),
    model_apply_deferred(S, D).
