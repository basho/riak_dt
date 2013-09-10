%% -------------------------------------------------------------------
%%
%% riak_dt_ts_gc: A CRDT for removing tombstones from set / map CRDTs
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_dt_ts_gc).

-compile(export_all).

-record(gc, {epoch = 0 :: non_neg_integer(),
             type :: riak_dt:crdt(), %% the type for the GLB
             all_glb   :: riak_dt_enable_flag:flag(),
             glb_members :: riak_dt_gset:gset(),
             all_removed :: riak_dt_enable_flag:flag(),
             removed_members ::riak_dt_gset:gset(),
             glb :: glb()}).

%%-type gc() :: #gc{}.

%% A Fragment of a CRDT, merging these fragments gets a GLB, not a LUB
-type glb()   :: riak_dt_orset:orset() |
                 riak_dt_vvorset:vvorset() |
                 riak_dt_multi:multi().

new(Type) ->
    #gc{type=Type}.




