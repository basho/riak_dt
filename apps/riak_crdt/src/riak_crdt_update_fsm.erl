%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% co-ordinator for a CRDT update operation
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(riak_crdt_update_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting_remotes/2]).

-record(state, {req_id :: pos_integer(),
                from :: pid(),
                key :: string(),
                args = undefined :: term() | undefined,
                mod :: atom(),
                preflist :: riak_core_apl:preflist2(),
                coord_pl_entry :: {integer(), atom()},
                num_w = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(ReqID, From, Key, Mod, Args) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Key, Mod, Args], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Key, Mod, Args]) ->
    SD = #state{req_id=ReqID,
                from=From,
                key=Key,
                mod=Mod,
                args=Args},
    {ok, prepare, SD, 0}.

%% @doc Prepare the update by calculating the _preference list_.
prepare(timeout, SD0=#state{key=Key, from=From, mod=Mod, args=Args, req_id=ReqId}) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    DocIdx = riak_core_util:chash_key({Key, Key}),
    UpNodes = riak_core_node_watcher:nodes(riak_crdt),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, 3, Ring, UpNodes),
    %% Check if this node is in the preference list so it can coordinate
    LocalPL = [IndexNode || {{_Index, Node} = IndexNode, _Type} <- Preflist2,
                            Node == node()],
    case {Preflist2, LocalPL =:= []} of
        {[], _} ->
            %% Empty preflist
            From ! {ReqId, {error, all_nodes_down}},
            {stop, error, SD0};
        {_, true} ->
            %% This node is not in the preference list
            %% forward on to the first node
            [{{_Idx, CoordNode},_Type}|_] = Preflist2,
            case riak_crdt_update_fsm_sup:start_update_fsm(CoordNode, [ReqId, From, Key, Mod, Args]) of
                {ok, _Pid} ->
                    {stop, normal, SD0};
                {error, Reason} ->
                    lager:error("Unable to forward update for ~p to ~p - ~p\n",
                                [Key, CoordNode, Reason]),
                    From ! {ReqId, {error, {coord_handoff_failed, Reason}}},
                    {stop, error, SD0}
            end;
        _ ->
            CoordPLEntry = hd(LocalPL),
            %% This node is in the preference list, continue
            SD = SD0#state{coord_pl_entry = CoordPLEntry, preflist = Preflist2},
            {next_state, execute, SD, 0}
    end.


%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{coord_pl_entry=CoordNode,
                            preflist=PrefList,
                            key=Key, mod=Mod,
                            args=Args, from=From, req_id=ReqId}) ->
    case riak_crdt_vnode:update(CoordNode, Key, Mod, Args) of
        {ok, CRDT} ->
            %% ask remote nodes to merge
            PrefList2 = [IndexNode || {IndexNode, _Type} <- PrefList],
            riak_crdt_vnode:merge(PrefList2, Key, CRDT, ReqId),
            {next_state, waiting_remotes, SD0#state{num_w=1}};
        Error ->
            %% send reply and bail
            From ! {ReqId, Error},
            {stop, normal, SD0}
    end. 

%% @doc Wait for at least 1 successfull merge reqs to respond.
%% TODO: What about merge errors?
waiting_remotes({ReqId, ok}, SD0=#state{from=From, num_w=NumW0}) ->
    NumW = NumW0 + 1,
    SD = SD0#state{num_w=NumW},
    if
        NumW =:= 2 ->
            From ! {ReqId, ok},
            {stop, normal, SD};
        true ->
            {next_state, waiting, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
