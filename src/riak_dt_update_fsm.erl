%% -------------------------------------------------------------------
%%
%% riak_dt_update_fsm: Co-ordinating, per request, state machine for updates
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_dt_update_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/6]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting_remotes/2]).

-type option() :: {timeout, pos_integer() | infinity}. %% overall time to wait

-record(state, {req_id :: pos_integer(),
                from :: pid(),
                key :: binary(),
                args = undefined :: term() | undefined,
                mod :: atom(),
                options = [] :: [option()],
                preflist :: riak_core_apl:preflist2(),
                coord_pl_entry :: {integer(), node()},
                num_w = 0 :: non_neg_integer(),
                timeout :: pos_integer() | infinity,
                tref :: reference()}).%% Timer reference

-define(FSM_TIMEOUT, 60000). %% a minute

%%%===================================================================
%%% API
%%%===================================================================
start_link(ReqID, From, Mod, Key, Args, Timeout) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Mod, Key, Args, [{timeout, Timeout}]], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Mod, Key, Args, Opts]) ->
    Timeout = get_timeout(Opts),
    TRef = schedule_timeout(Timeout),
    SD = #state{req_id=ReqID,
                from=From,
                key=Key,
                mod=Mod,
                args=Args,
                options=Opts,
                tref=TRef,
                timeout=Timeout},
    {ok, prepare, SD, 0}.

%% @doc Prepare the update by calculating the preference list.
prepare(timeout, SD0=#state{key=Key, from=From, mod=Mod, args=Args,
                            req_id=ReqId, timeout=Timeout}) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    DocIdx = riak_core_util:chash_key({Mod, Key}),
    UpNodes = riak_core_node_watcher:nodes(riak_dt),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, 3, Ring, UpNodes),
    %% Check if this node is in the preference list so it can coordinate
    LocalPL = [IndexNode || {{_Index, Node} = IndexNode, _Type} <- Preflist2,
                            Node == node()],
    Primaries = [IndexNode || {IndexNode, primary} <- Preflist2 ],
    case {Primaries, Preflist2, LocalPL =:= []} of
        {_, [], _} ->
            %% Empty preflist, no replicas available
            client_reply({error, all_nodes_down}, SD0),
            {stop, error, SD0};
        {[], _, _} ->
            %% There must be at least one primary in the preflist so
            %% we can reduce garbage accretion resulting from many
            %% vnodes being involved in writes. This is a separate
            %% constraint from the local node being in the preflist.
            %% Ideally, we forward to a primary.
            client_reply({error, no_primaries}, SD0),
            {stop, error, SD0};
        {_, _, true} ->
            %% This node is not in the preference list
            %% forward on to the first node
            [{{_Idx, CoordNode},_Type}|_] = Preflist2,
            case riak_dt_update_fsm_sup:start_update_fsm(CoordNode, [ReqId, From, Mod, Key,
                                                                     Args, Timeout]) of
                {ok, _Pid} ->
                    {stop, normal, SD0};
                {error, Reason} ->
                    lager:error("Unable to forward update for ~p to ~p - ~p\n",
                                [Key, CoordNode, Reason]),
                    client_reply({error, {coord_handoff_failed, Reason}}, SD0),
                    {stop, error, SD0}
            end;
        _ ->
            CoordPLEntry = hd(LocalPL),
            %% This node is in the preference list, continue
            SD = SD0#state{coord_pl_entry = CoordPLEntry, preflist = Preflist2},
            {next_state, execute, SD, 0}
    end.


%% @doc Execute the write request and then go into waiting state to
%% verify it meets consistency requirements.
execute(timeout, SD=#state{coord_pl_entry=CoordPLEntry,
                            preflist=PrefList,
                            key=Key, mod=Mod, timeout=Timeout,
                            args=Args, req_id=ReqId}) ->
    case riak_dt_vnode:update(CoordPLEntry, Mod, Key, Args, Timeout) of
        {ok, CRDT} ->
            %% ask remote nodes to merge
            PrefList2 = [{Index, Node} || {{Index, Node}=Entry, _Type} <- PrefList,
                                          Entry =/= CoordPLEntry],
            riak_dt_vnode:merge(PrefList2, Mod, Key, CRDT, ReqId),
            {next_state, waiting_remotes, SD#state{num_w=1}};
        Error ->
            %% send reply and bail
            client_reply(Error, SD),
            {stop, normal, SD}
    end.

%% @doc Wait for at least 1 successfull merge req to respond.
%% TODO: What about merge errors? hrm
waiting_remotes({_ReqId, ok}, SD0=#state{num_w=NumW0}) ->
    NumW = NumW0 + 1,
    SD = SD0#state{num_w=NumW},
    if
        NumW =:= 2 ->
            client_reply(ok, SD),
            {stop, normal, SD};
        true ->
            {next_state, waiting_remotes, SD}
    end;
waiting_remotes(request_timeout, SD) ->
    client_reply({error, timeout}, SD),
    {stop, normal, SD}.

handle_info(request_timeout, StateName, SD) ->
    ?MODULE:StateName(request_timeout, SD);
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, #state{tref=TRef}) ->
    cancel_timeout(TRef),
    ok.

schedule_timeout(infinity) ->
    undefined;
schedule_timeout(Timeout) when is_integer(Timeout) ->
    gen_fsm:send_event_after(Timeout, request_timeout).

cancel_timeout(TRef) when is_reference(TRef) ->
    gen_fsm:cancel_timer(TRef);
cancel_timeout(_) ->
    ok.

%% internal
get_timeout(Opts) ->
    FSMTimeout = app_helper:get_env(riak_dt, update_fsm_timeout, ?FSM_TIMEOUT),
    proplists:get_value(timeout, Opts, FSMTimeout).

client_reply(Reply, #state{from=From, req_id=ReqId}) ->
    From ! {ReqId, Reply}.
