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
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         update/3,
         update/4,
         value/2,
         value/3
        ]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Tell the crdt at key to update itself with Args.
update(Mod, Key, Args) ->
    update(Mod, Key, Args, 5000).
update(Mod, Key, Args, Timeout) ->
    ReqID = mk_reqid(),
    {ok, _} = riak_dt_update_fsm_sup:start_update_fsm(node(), [ReqID, self(), Mod, Key, Args]),
    wait_for_reqid(ReqID, Timeout).
value(Mod, Key) ->
    value(Mod, Key, 5000).
value(Mod, Key, Timeout) ->
    ReqID = mk_reqid(),
    {ok, _} = riak_dt_value_fsm_sup:start_value_fsm(node(), [ReqID, self(), Mod, Key]),
    wait_for_reqid(ReqID, Timeout).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
mk_reqid() -> erlang:phash2(erlang:now()).

wait_for_reqid(ReqID, Timeout) ->
    receive
        {ReqID, ok} -> ok;
        {ReqID, ok, Val} -> {ok, Val};
        {ReqID, Error} -> Error;
        Other ->
            lager:error("Received unexpected message! ~w~n", [Other]),
            Other
    after Timeout ->
	    {error, timeout}
    end.
