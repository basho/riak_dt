%% -------------------------------------------------------------------
%%
%% riak_dt_client: local client for riak_dt
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

%% @doc Interface into the CRDT application.
-module(riak_dt_client).
-include("riak_dt.hrl").

-export([
         update/3,
         update/4,
         value/2,
         value/3
        ]).

-define(REQ_TIMEOUT, 60000). %% default to a minute

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Tell the crdt at key to update itself with Args.
update(Mod, Key, Args) ->
    update(Mod, Key, Args, ?REQ_TIMEOUT).
update(Mod, Key, Args, Timeout) ->
    ReqID = mk_reqid(),
    {ok, _} = riak_dt_update_fsm_sup:start_update_fsm(node(), [ReqID, self(), Mod, Key, Args, Timeout]),
    wait_for_reqid(ReqID, Timeout).
value(Mod, Key) ->
    value(Mod, Key, ?REQ_TIMEOUT).
value(Mod, Key, Timeout) ->
    ReqID = mk_reqid(),
    {ok, _} = riak_dt_value_fsm_sup:start_value_fsm(node(), [ReqID, self(), Mod, Key, Timeout]),
    wait_for_reqid(ReqID, Timeout).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
mk_reqid() -> erlang:phash2(erlang:now()).

wait_for_reqid(ReqID, Timeout0) ->
    %% give the fsm 100ms to reply before we cut it off
    Timeout = if is_integer(Timeout0) ->
            Timeout0 + 100;
                 true -> Timeout0
              end,
    receive
        {ReqID, Response} -> Response
    after Timeout ->
            %% Do we need to do something to drain messages on the queue,
            %% if the FSM replies after we timeout?
	    {error, timeout}
    end.
