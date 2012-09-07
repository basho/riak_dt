%% -------------------------------------------------------------------
%%
%% riak_dt_demo: test demo code for partition @TODO drop this file
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

-module(riak_dt_demo).

-export([part/0, heal/0]).

part() ->
    true = rpc:call('riak_dt2@127.0.0.1', erlang, set_cookie, ['riak_dt2@127.0.0.1', riak_dt2]),
    true = erlang:set_cookie(node(), riak_dt2),
    true = rpc:call('riak_dt2@127.0.0.1', erlang, disconnect_node, ['riak_dt3@127.0.0.1']),
    true = rpc:call('riak_dt2@127.0.0.1', erlang, disconnect_node, ['riak_dt4@127.0.0.1']),
    true = erlang:disconnect_node('riak_dt3@127.0.0.1'),
    true = erlang:disconnect_node('riak_dt4@127.0.0.1').


heal() ->
    true = rpc:call('riak_dt2@127.0.0.1', erlang, set_cookie, ['riak_dt2@127.0.0.1', riak_dt]),
    true = erlang:set_cookie(node(), riak_dt).
