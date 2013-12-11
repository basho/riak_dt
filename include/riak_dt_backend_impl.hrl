%% -------------------------------------------------------------------
%%
%% riak_dt_backend_impl: Selecting the backends for our riak_dt data types
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

%% We want to measure the performance difference of riak_dt when it uses
%% orddict (and ordsets) vs when it uses dict (and sets). This include file
%% is where you can change which underlying implementation it uses.
%%
%% Beware subtle differences between the interfaces. We should be largely ok, but
%% there are differences in key equality between implementations, as well as a
%% definition of overall equality. We can't do much about the first one, but we are
%% doing something about the second with dict_equal and sets_equal.

% The module to use for dictionaries
-define(DT_ERL_DICT, orddict). % OR dict
-type dt_erl_dict_type() :: orddict:orddict(). % OR dict()
-define(DT_ERL_DICT_EQUAL(D1, D2), 
    case ?DT_ERL_DICT of
        orddict -> D1 == D2;
        dict    -> lists:usort(dict:to_list(D1)) == lists:usort(dict:to_list(D2))
    end).

% The module to use for sets
-define(DT_ERL_SETS, ordsets). % OR sets
-type dt_erl_sets_type(T) :: ordsets:ordset(T). % OR set()
-define(DT_ERL_SETS_EQUAL(S1, S2), 
    case ?DT_ERL_SETS of
        ordsets -> S1 == S2;
        sets    -> lists:usort(sets:to_list(S1)) == lists:usort(sets:to_list(S1))
    end).
    

