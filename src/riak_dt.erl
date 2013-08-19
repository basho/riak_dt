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

-export([behaviour_info/1]).

-ifndef(EQC).
-define(EQC_DT_CALLBACKS, []).
-else.
% Extra callbacks for any crdt_statem_eqc tests
-define(EQC_DT_CALLBACKS, [{gen_op, 0}, 
                           {update_expected, 3}, 
                           {eqc_state_value, 1}, 
                           {init_state, 0}]).
-endif.

behaviour_info(callbacks) ->
    [{new, 0},
     {value, 1},
     {value, 2},
     {update, 3},
     {merge, 2},
     {equal, 2},
     {to_binary, 1},
     {from_binary, 1} | ?EQC_DT_CALLBACKS ].
