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

-module(riak_dt_ricon_demo).

-compile(export_all).

-define(CLUSTER, ['riak@mgmt.r1s12', 'riak@mgmt.r1s13', 'riak@mgmt.r1s14', 'riak@mgmt.r1s15']).
%%-define(CLUSTER, ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1', 'dev4@127.0.0.1']).

part() ->
    {P1, P2} = lists:split(2, ?CLUSTER),
    OldCookie = rpc:call(hd(P1), erlang, get_cookie, []),
    NewCookie = list_to_atom(lists:reverse(atom_to_list(OldCookie))),
    [true = rpc:call(N, erlang, set_cookie, [N, NewCookie]) || N <- P1],
    [[true = rpc:call(N, erlang, disconnect_node, [P2N]) || N <- P1] || P2N <- P2],
    erlang:put(part_info, {P1, OldCookie}).

heal() ->
    {P1, OldCookie} = erlang:get(part_info),
    [true = rpc:call(N, erlang, set_cookie, [N, OldCookie]) || N <- P1],
    wait_util_connected(),
    {_GN, []} = rpc:sbcast(?CLUSTER, riak_core_node_watcher, broadcast),
    wait_for_cluster_service().

wait_util_connected() ->
    F = fun(Node) ->
                Connected = rpc:call(Node, erlang, nodes, []),
                lists:sort(?CLUSTER) == lists:sort([Node]++Connected)
        end,
    [ok = wait_until(Node, F) || Node <- ?CLUSTER],
    ok.

wait_for_cluster_service() ->
    F = fun(N) ->
                UpNodes = rpc:call(N, riak_core_node_watcher, nodes, [riak_dt]),
                (?CLUSTER -- UpNodes) == []
        end,
    [ok = wait_until(Node, F) || Node <- ?CLUSTER],
    ok.

wait_until(Node, Fun) ->
    wait_until(Node, Fun, 5000, 500).

wait_until(Node, Fun, Retry, Delay) ->
    Pass = Fun(Node),
    case {Retry, Pass} of
        {_, true} ->
            ok;
        {0, _} ->
            fail;
        _ ->
            timer:sleep(Delay),
            wait_until(Node, Fun, Retry-1, Delay)
    end.
