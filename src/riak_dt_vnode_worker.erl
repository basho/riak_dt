%% -------------------------------------------------------------------
%%
%% riak_dt_vnode_worker: Vnode worker for async folds
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
-module(riak_dt_vnode_worker).

-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(state, {index :: partition()}).

init_worker(Index, _Args, _Props) ->
    {ok, #state{index=Index}}.

handle_work({fold, FoldFun}, _Sender, State) ->
    {reply, FoldFun(), State}.
