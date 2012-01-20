%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2012, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 19 Jan 2012 by Russell Brown <russelldb@basho.com>

-module(pncounter_statem_eqc).


-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-record(state,{vnodes=[], expected_value=0, vnode_id=0, mod}).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%% Initialize the state
initial_state() ->
    #state{}.

%% Command generator, S is the state
command(#state{vnodes=VNodes, mod=Mod}) ->
    oneof([{call, ?MODULE, create, [Mod]}] ++
           [{call, ?MODULE, update, [Mod, Mod:gen_op(), elements(VNodes)]} || length(VNodes) > 0] ++ %% If a vnode exists
           [{call, ?MODULE, merge, [Mod, elements(VNodes), elements(VNodes)]} || length(VNodes) > 0] ++
           [{call, ?MODULE, crdt_equals, [Mod, elements(VNodes), elements(VNodes)]} || length(VNodes) > 0]
).

%% Next state transformation, S is the current state
next_state(#state{vnodes=VNodes, vnode_id=ID}=S,V,{call,?MODULE,create,_}) ->
    S#state{vnodes=VNodes++[{ID, V}], vnode_id=ID+1};
next_state(#state{vnodes=VNodes0, expected_value=Expected, mod=Mod}=S,V,{call,?MODULE, update, [Op, {ID, _C}]}) ->
    VNodes = lists:keyreplace(ID, 1, VNodes0, {ID, V}),
    S#state{vnodes=VNodes, expected_value=Mod:update_expected(Op, Expected)};
next_state(#state{vnodes=VNodes0}=S,V,{call,?MODULE, merge, [_Source, {ID, _C}]}) ->
    VNodes = lists:keyreplace(ID, 1, VNodes0, {ID, V}),
    S#state{vnodes=VNodes};
next_state(S, _V, _) ->
    S.

%% Precondition, checked before command is added to the command sequence
precondition(_S,{call,_,_,_}) ->
    true.

%% Postcondition, checked after command has been evaluated
%% OBS: S is the state before next_state(S,_,<command>) 
postcondition(_S,{call,?MODULE, crdt_equals,_},Res) ->
    Res==true;
postcondition(_S,{call,_,_,_},_Res) ->
    true.

prop_converge(NumTests, Mod) ->
    eqc:quickcheck(eqc:numtests(NumTests, ?QC_OUT(prop_converge(Mod)))).

prop_converge(Mod) ->
    ?FORALL(Cmds,commands(?MODULE, #state{mod=Mod}),
            begin
                {H,S,Res} = run_commands(?MODULE,Cmds),
                Merged = merge_crdts(Mod, S#state.vnodes),
                MergedVal = Mod:value(Merged),
                ?WHENFAIL(
                   io:format("History: ~p\nState: ~p\nRes: ~p\n",[H,S,Res]),
                   conjunction([{res, equals(Res, ok)},
                                {total, equals(MergedVal, S#state.expected_value)}]))
            end).

merge_crdts(Mod, []) ->
    Mod:new();
merge_crdts(Mod, [{_ID, Crdt}|Crdts]) ->
    lists:foldl(fun({_ID0, C}, Acc) ->
                        Mod:merge(C, Acc) end,
                Crdt,
                Crdts).

%% Commands
create(Mod) ->
    Mod:new().

update(Mod, Op, {ID, C}) ->
    Mod:update(Op, ID, C).

merge(Mod, {_IDS, CS}, {_IDD, CD}) ->
    Mod:merge(CS, CD).

crdt_equals(Mod, {_IDS, CS}, {_IDD, CD}) ->
    Mod:equal(Mod:merge(CS, CD),
              Mod:merge(CD, CS)).


