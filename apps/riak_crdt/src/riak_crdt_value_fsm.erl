%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% co-ordinator for a CRDT update operation
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(riak_crdt_value_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

-record(state, {req_id :: pos_integer(),
                from :: pid(),
                mod :: atom(),
                key :: string(),
                values=[] :: [term()],
                preflist :: riak_core_apl:preflist2(),
                coord_pl_entry :: {integer(), atom()},
                num_r = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(ReqID, From, Mod, Key) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Mod, Key], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Mod, Key]) ->
    SD = #state{req_id=ReqID,
                from=From,
                mod=Mod,
                key=Key},
    {ok, prepare, SD, 0}.

%% @doc Prepare the update by calculating the _preference list_.
prepare(timeout, SD0=#state{mod=Mod, key=Key}) ->
    {ok,Ring} = riak_core_ring_manager:get_my_ring(),
    DocIdx = riak_core_util:chash_key({Mod, Key}),
    UpNodes = riak_core_node_watcher:nodes(riak_crdt),
    Preflist2 = riak_core_apl:get_apl_ann(DocIdx, 3, Ring, UpNodes),
    Preflist = [IndexNode || {IndexNode, _Type} <- Preflist2],
    SD = SD0#state{ preflist = Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{preflist=Preflist, mod=Mod, key=Key, req_id=ReqId}) ->
    riak_crdt_vnode:value(Preflist, Mod, Key, ReqId),
    {next_state, waiting, SD0}.

%% @doc Gather some responses, and merge them
waiting({ReqId, {ok, {Mod, Val}}}, SD0=#state{from=From, num_r=NumR0, mod=Mod, values=Values}) ->
    NumR = NumR0 + 1,
    Values2 = [Val|Values],
    SD = SD0#state{num_r=NumR, values=Values2},
    if
        NumR =:= 2 ->
            [V1, V2] = Values2,
            Merged = Mod:merge(V1, V2),
            From ! {ReqId, Mod:value(Merged)},
            {stop, normal, SD};
        true ->
            {next_state, waiting, SD}
    end;
waiting({_ReqId, notfound}, SD) ->
    {next_state, waiting, SD}.

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.
