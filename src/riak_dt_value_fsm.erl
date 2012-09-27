%% -------------------------------------------------------------------
%%
%% riak_dt_value_fsm: Per request value fsm
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

-module(riak_dt_value_fsm).

-behaviour(gen_fsm).

%% API
-export([start_link/5]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, await_r/2, await_n/2, read_repair/2]).

-define(FSM_TIMEOUT, 60000). %% a minute

-type option() :: {timeout, pos_integer() | infinity}. %% overall time to wait
                                                        %% for vnode repsonses
-type tref() :: reference() | undefined.

-record(state, {req_id :: pos_integer(),
                from :: pid(),
                mod :: atom(),
                key :: binary(),
                replies=[] :: [{integer(), term()}],
                preflist :: riak_core_apl:preflist2(),
                coord_pl_entry :: {integer(), atom()},
                num_r = 0 :: non_neg_integer(),
                options = [] ::  option(),
                fsm_timeout :: pos_integer(),
                req_timeout :: pos_integer(),
                trefs = [] :: [tref()],
                send_reply = true :: boolean()}). %% should the fsm send the client a response?

%%%===================================================================
%%% API
%%%===================================================================
start_link(ReqID, From, Mod, Key, Timeout) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Mod, Key, [{timeout, Timeout}]], []).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Mod, Key, Options]) ->
    SD = #state{req_id=ReqID,
                from=From,
                mod=Mod,
                key=Key,
                options = Options},
    {ok, prepare, SD, 0}.

%% @doc Prepare the read
prepare(timeout, SD0=#state{mod=Mod, key=Key, options=Options}) ->
    {ReqTimeout, FSMTimeout} = get_timeouts(Options),
    Preflist = get_preflist(Mod, Key),
    SD = SD0#state{ preflist = Preflist, fsm_timeout=FSMTimeout, req_timeout=ReqTimeout},
    {next_state, execute, SD, 0}.

%% @doc Execute the read request and then go into waiting state for responses
execute(timeout, SD=#state{preflist=Preflist, mod=Mod, key=Key, req_id=ReqId,
                           req_timeout=ReqTimeout, fsm_timeout=FSMTimeout}) ->
    TRefs = schedule_timeouts([{request_timeout, ReqTimeout}, {fsm_timeout, FSMTimeout}]),
    riak_dt_vnode:value(Preflist, Mod, Key, ReqId),
    {next_state, await_r, SD#state{trefs=TRefs}}.

%% @doc Gather some responses, and merge them
await_r({ReqId, Reply}, SD0=#state{req_id=ReqId, num_r=NumR0, replies=Replies}) ->
    NumR = NumR0 + 1,
    Replies2 = [Reply|Replies],
    SD = SD0#state{num_r=NumR, replies=Replies2},
    if
        NumR =:= 2 ->
            Result = value(Replies2),
            client_reply(Result, SD),
            {next_state, await_n, SD};
        true ->
            {next_state, await_r, SD}
    end;
await_r(request_timeout, SD) ->
    client_reply({error, timeout}, SD),
    {next_state, await_r, SD#state{send_reply=false}};
await_r(fsm_timeout, SD) ->
    {stop, normal, SD}.

await_n({_ReqId, Reply}, SD0=#state{num_r=NumR0, replies=Replies0}) ->
    NumR = NumR0 +1,
    Replies = [Reply|Replies0],
    SD = SD0#state{num_r=NumR, replies=Replies},
    if NumR =:= 3 ->
            {next_state, read_repair, SD, 0};
       true ->
            {next_state, await_n, SD}
    end;
await_n(request_timeout, SD) ->
    {next_state, await_n, SD};
await_n(fsm_timeout, SD) ->
    {stop, normal, SD}.

%% rr
read_repair(timeout, SD=#state{mod=Mod, key=Key, replies=Replies}) ->
    Merged = merge(Replies, notfound),
    PrefList = [IdxNode || {IdxNode, Val} <- Replies, needs_repair(Val, Merged)],
    do_repair(PrefList, Mod, Key, Merged),
    {stop, normal, SD}.

%% @private
handle_info(request_timeout, StateName, StateData) ->
    ?MODULE:StateName(request_timeout, StateData);
handle_info(fsm_timeout, StateName, StateData) ->
    ?MODULE:StateName(fsm_timeout, StateData);
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, #state{trefs=Trefs}) ->
    [cancel_timeout(Tref) || Tref <- Trefs].

%% internal
%% get a single merged value for a list of replies
value(Replies) ->
    case merge(Replies, notfound) of
        {error, Reason} -> {error, Reason};
        {ok, {Mod, Val}} -> Mod:value(Val);
        notfound -> notfound
    end.

%% merge all replies to a single CRDT
merge([], Final) ->
    Final;
merge([{_Idx, {ok, {Mod, Val1}}}|Rest], {ok, {Mod, Val2}}) ->
    Mergedest = Mod:merge(Val1, Val2),
    merge(Rest, {ok, {Mod, Mergedest}});
merge([{_Idx, _}|Rest], {ok, _}=Mergedest) ->
    merge(Rest, Mergedest);
merge([{_Idx, {ok, _}=Mergedest}|Rest], _) ->
    merge(Rest, Mergedest);
merge([{_Idx, _}|Rest], {error, Error}) ->
    merge(Rest, {error, Error});
merge([{_Idx, A}|Rest], _) ->
    merge(Rest, A).

%% Does Val1 need repairing?
needs_repair({ok, {Mod, Val1}}, {ok, {Mod, Val2}}) ->
    Mod:equal(Val1, Val2) =:= false;
needs_repair(_, {ok, _}) ->
    true;
needs_repair(_, _) ->
    false.

do_repair(Indexes, Mod, Key, {ok, Merged}) when length(Indexes) =/= 0 ->
    riak_dt_vnode:repair(Indexes, Mod, Key, Merged);
do_repair(_, _, _, _) ->
    ok.

get_timeouts(Options) ->
    %% @TODO add this to app.config, or at least document it
    FSMTimeout0 = app_helper:get_env(riak_dt, value_fsm_timeout, ?FSM_TIMEOUT),
    ReqTimeout = proplists:get_value(timeout, Options, FSMTimeout0),
    FSMTimeout = fsm_timeout(ReqTimeout, FSMTimeout0),
    {ReqTimeout, FSMTimeout}.

fsm_timeout(ReqTimeout, FSMTimeout0) ->
    Max = timeout_max(ReqTimeout, FSMTimeout0),
    fsm_timeout(Max).

fsm_timeout(infinity) ->
    infinity;
%% fsm timeout must be greater than ReqTimeout
%% so we never need send a message on fsm timeout
%% so add 100ms grace for the FSM
fsm_timeout(N) when is_integer(N) ->
    N + 100.

timeout_max(undefined, B) ->
    B;
timeout_max(A, undefined) ->
    A;
timeout_max(infinity, _) ->
    infinity;
timeout_max(_, infinity) ->
    infinity;
timeout_max(A, B) when is_integer(A), is_integer(B) ->
    erlang:max(A, B).

get_preflist(Mod, Key) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    DocIdx = riak_core_util:chash_key({Mod, Key}),
    UpNodes = riak_core_node_watcher:nodes(riak_dt),
    Preflist0 = riak_core_apl:get_apl_ann(DocIdx, 3, Ring, UpNodes),
    [IndexNode || {IndexNode, _Type} <- Preflist0].

schedule_timeouts(Timeouts) ->
    [schedule_timeout(Message, Timeout) || {Message, Timeout} <- Timeouts].

%% Send self a message after Timeout
schedule_timeout(_Message, infinity) ->
    undefined;
schedule_timeout(Message, Timeout) ->
    erlang:send_after(Timeout, self(), Message).

cancel_timeout(TRef) when is_reference(TRef) ->
    erlang:cancel_timer(TRef);
cancel_timeout(_) ->
    ok.

client_reply(_Reply, #state{send_reply = false}) ->
    ok; %% don't reply
client_reply(Reply, #state{from = From, req_id = ReqId}) ->
    From ! {ReqId, Reply}.
