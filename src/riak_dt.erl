%% -------------------------------------------------------------------
%%
%% riak_dt.erl: behaviour for convergent data types
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

-module(riak_dt).

-export_type([crdt/0, actor/0]).

-type crdt() :: term().
-type operation() :: term().
-type actor() :: term().

-callback new() -> crdt().
-callback value(crdt()) -> term().
-callback value(term(), crdt()) -> term().
-callback update(operation(), actor(), crdt()) -> crdt().
-callback merge(crdt(), crdt()) -> crdt().
-callback equal(crdt(), crdt()) -> boolean().
-callback to_binary(crdt()) -> binary().
-callback from_binary(binary()) -> crdt().

-ifdef(EQC).
% Extra callbacks for any crdt_statem_eqc tests
-type model_state() :: term().

-callback gen_op() -> eqc_gen:gen(operation()).
-callback update_expected(actor(), operation(), model_state()) -> model_state().
-callback eqc_state_value(model_state()) -> term().
-callback init_state() -> model_state().

-endif.
