%% -------------------------------------------------------------------
%%
%% riak_dt.hrl: include header file
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

%% Vnode Request for fetching the value of a data-structure.
-record(value, {
          module :: module(), %% The datatype to update
          key    :: binary(), %% The key of the data structure
          req_id :: pos_integer() %% A request identifier
         }).

%% Vnode Request for updating (or creating) a data-structure.
-record(update, {
          module :: module(), %% The datatype to update
          key    :: binary(), %% The key of the data structure
          args   :: [ term() ] %% Arguments to the data structure's update function, i.e. what to change
         }).

%% Vnode Request for merging a remote value into the local state for a
%% data-structure.
-record(merge, {
          module :: module(), %% The datatype to update
          key    :: binary(), %% The key of the data structure
          value  :: {module(), term()}, %% The value from the remote replica
          req_id :: pos_integer() %% A request identifier
         }).

%% Shorthands for request records. This will be useful at a later date
%% when we need to deprecate internal request formats.
-define(VALUE_REQ, #value).
-define(UPDATE_REQ, #update).
-define(MERGE_REQ, #merge).
