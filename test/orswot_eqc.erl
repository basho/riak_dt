%% -------------------------------------------------------------------
%%
%% orswot_statem: Try and catch that merge bug I introduced.
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

-record(state,{vnodes=[] :: [binary()], %% Sort of like the ring, upto N*2 vnodeids
               vnode_data=[] :: [{VNodeID :: binary(),
                                  riak_dt_orswot:orswot(),
                                  riak_dt_orset:orset()}],
               %% The data, duplicated values for vnodes
               %% Newest at the head of the list.
               %% Prepend only data 'cos symbolic / dynamic state.
               last_get=undefined :: undefined | {undefined | riak_dt_orswot:orswot(), undefined | riak_dt_orset:orset()},
               %% Stash the last `get' result so we can use it to put later
               n=0 :: integer(), %% Generated NVal
               r=0 :: integer(), %% Generated R quorum
               time=0 :: integer(),  %% For the vclocks
               adds=[] :: [{VNodeID :: binary(), atom()}] %% things that have been added
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
%% Only set N, R if N has not been set (ie run once, as the first command)
set_nr_pre(#state{n=N}) ->
     N == 0.

set_nr_args(_S) ->
    [?LET(NVal, choose(2, 4), {NVal, choose(1, NVal)})].

set_nr(_) ->
    %% Command args used for next state only
    ok.

set_nr_next(S, _V, [{N, R}]) ->
    S#state{n=N, r=R}.

%% ------ Grouped operator: make_ring
%% Generate a bunch of vnodes, only runs until enough are generated
make_ring_pre(#state{vnodes=VNodes, n=N}) ->
    N > 0 andalso length(VNodes) < N * 2.

make_ring_args(#state{vnodes=VNodes, n=N}) ->
    [VNodes, vector(N, binary(8))].

make_ring(_,_) ->
    %% Command args used for next state only
    ok.

make_ring_next(S=#state{vnodes=VNodes}, _V, [_, NewVNodes0]) ->
    %% No duplicate vnode ids please!
    NewVNodes = lists:filter(fun(Id) -> not lists:member(Id, VNodes) end, NewVNodes0),
    S#state{vnodes=VNodes ++ NewVNodes}.

%% ------ Grouped operator: put
%% Store a new value, a contextless put
add_pre(S) ->
    vnodes_ready(S).

add_args(#state{vnodes=VNodes, vnode_data=VNodeData}) ->
    [
     oneof(['X', 'Y', 'Z']), %% a new value
     elements(VNodes), % The replica
     VNodeData %% The existing vnode data
    ].

%% Add a value to the set
add(Value, Actor, VNodeData) ->
    {ORSWOT, ORSet} = get(Actor, VNodeData),
    {ok, ORSWOT2} = riak_dt_orswot:update({add, Value}, Actor, ORSWOT),
    {ok, ORSet2} = riak_dt_orset:update({add, Value}, Actor, ORSet),
    {Actor, ORSWOT2, ORSet2}.

add_next(S=#state{vnode_data=VNodeData, adds=Adds}, Res, [Value, Coord, _]) ->
    %% The state data is prepend only, it grows and grows, but it's based on older state
    %% Newest at the front.
    S#state{vnode_data=[Res | VNodeData], adds=[{Coord, Value} | Adds]}.

add_post(_S, _Args, Res) ->
    post_all(Res, add).

%% ------ Grouped operator: remove
%% remove, but only something that has been added already
remove_pre(S=#state{adds=Adds}) ->
    %% Only do an update if you already did a get
    vnodes_ready(S) andalso Adds /= [].

remove_args(#state{adds=Adds, vnode_data=VNodeData}) ->
    [
     elements(Adds), %% Something that has been added
     VNodeData %% All the vnode data
    ].

remove_pre(#state{adds=Adds}, [Add, _]) ->
    lists:member(Add, Adds).

remove({VNode, Value}, VNodeData) ->
    {ORSWOT, ORSet} = get(VNode, VNodeData),
    %% even though we only remove what has been added, there is no
    %% guarantee a merge from another replica hasn't led to the
    %% element being absent already, so ignore precon errors (they
    %% don't change state)
    ORSWOT2 = ignore_preconerror_remove(Value, VNodeData, ORSWOT, riak_dt_orswot),
    ORSet2 = ignore_preconerror_remove(Value, VNodeData, ORSet, riak_dt_orset),
    {VNode, ORSWOT2, ORSet2}.

ignore_preconerror_remove(Value, Actor, Set, Mod) ->
    case Mod:update({remove, Value}, Actor, Set) of
        {ok, Set2} ->
            Set2;
        _ ->
            Set
    end.

remove_next(S=#state{vnode_data=VNodeData, adds=Adds}, Res, [Add, _]) ->
    %% The state data is prepend only, it grows and grows, but it's based on older state
    %% Newest at the front.
    S#state{vnode_data=[Res | VNodeData], adds=lists:delete(Add, Adds)}.

remove_post(_S, _Args, Res) ->
    post_all(Res, remove).

%% ------ Grouped operator: replicate
%% Merge two replicas values
replicate_pre(S=#state{vnode_data=VNodeData}) ->
    vnodes_ready(S) andalso VNodeData /= [].

replicate_args(#state{vnodes=VNodes, vnode_data=VNodeData}) ->
    [
     elements(VNodes), %% Replicate from
     elements(VNodes), %% replicate too
     VNodeData
    ].

replicate_pre(_S, [VN, VN, _]) ->
    false;
replicate_pre(_S, [_VN1, _VN2, _]) ->
    true.

%% Mutating multiple elements in vnode_data in place is bad idea
%% (symbolic vs dynamic state), so instead of treating Put and
%% replicate as the same action, this command handles the replicate
%% part of the put fsm. Data from some random vnode (From) is
%% replicated to some random vnode (To)
replicate(From, To, VNodeData) ->
    {FromORSWOT, FromORSet} = get(From, VNodeData),
    {ToORSWOT, ToORSet} = get(To, VNodeData),
    ORSWOT = riak_dt_orswot:merge(FromORSWOT, ToORSWOT),
    ORSet = riak_dt_orset:merge(FromORSet, ToORSet),
    {To, ORSWOT, ORSet}.

replicate_next(S=#state{vnode_data=VNodeData}, Res, _Args) ->
    S#state{vnode_data=[Res | VNodeData]}.

replicate_post(_S, _Args, Res) ->
    post_all(Res, rep).

%% Tests the property that an ORSWOT is equivalent to an ORSet
prop_merge() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                {H, S=#state{vnodes=VNodes, vnode_data=VNodeData}, Res} = run_commands(?MODULE,Cmds),
                %% Check that collapsing all values leads to the same results for dvv and riak_object
                {OV, ORV} = case VNodes of
                                [] ->
                                    {[], []};
                                _L ->
                                    %% Get ALL actor's values
                                    {OS, OR} = lists:foldl(fun(Actor, {O, OR}) ->
                                                                   {O1, OR1} = get(Actor, VNodeData),
                                                                   {riak_dt_orswot:merge(O, O1),
                                                                    riak_dt_orset:merge(OR, OR1)} end,
                                                           {riak_dt_orswot:new(), riak_dt_orset:new()},
                                                           VNodes),
                                    {riak_dt_orswot:value(OS), riak_dt_orset:value(OR)}
                            end,
                        aggregate(command_names(Cmds),
                                  pretty_commands(?MODULE,Cmds, {H,S,Res},
                                                  conjunction([{result,  equals(Res, ok)},
                                                               {values, equals(lists:sort(OV), lists:sort(ORV))}])
                                                 ))
            end).

%% -----------
%% Helpers
%% ----------
vnodes_ready(#state{vnodes=VNodes, n=N}) ->
    length(VNodes) >= N andalso N > 0.

post_all({_, ORSWOT, ORSet}, Cmd) ->
    %% What matters is that both types have the exact same results.
    case lists:sort(riak_dt_orswot:value(ORSWOT)) == lists:sort(riak_dt_orset:value(ORSet)) of
        true ->
            true;
        _ ->
            {postcondition_failed, "Swot and Set don't match", Cmd, ORSWOT, ORSet}
    end.


%% if a vnode does not yet have vnode data, return `undefined` for the
%% riak_object and DVVSet.
get(VNode, VNodeData) ->
    case lists:keyfind(VNode, 1, VNodeData) of
        {VNode, ORSWOT, ORSet} ->
            {ORSWOT, ORSet};
        false -> {riak_dt_orswot:new(), riak_dt_orset:new()}
    end.

-endif. % EQC
