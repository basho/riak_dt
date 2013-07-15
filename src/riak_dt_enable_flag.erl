%% -------------------------------------------------------------------
%%
%% riak_dt_enable_flag: A state based, enable-only flag.
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

%%% @doc
%%% an ON/OFF (boolean) CRDT, which converges towards ON
%%% it has a single operation: enable.
%%% @end

-module(riak_dt_enable_flag).

-behaviour(riak_dt).

-export([new/0, value/1, update/3, merge/2, equal/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% EQC generator
-ifdef(EQC).
gen_op() ->
    enable.

update_expected(_ID, enable, _Prev) ->
    on;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.
-endif.

new() ->
    off.

value(Flag) ->
    Flag.

update(enable, _Actor, _Flag) ->
    on.

merge(FA, FB) ->
    flag_or(FA, FB).

equal(FA,FB) ->
    FA =:= FB.

%% priv
flag_or(on, _) ->
    on;
flag_or(_, on) ->
    on;
flag_or(off, off) ->
    off.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(off, 1000, ?MODULE))]}.
-endif.

new_test() ->
    ?assertEqual(off, new()).

update_enable_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    ?assertEqual(on, F1).

update_enable_multi_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    F2 = update(enable, 1, F1),
    ?assertEqual(on, F2).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(off, merge(F0, F0)).

merge_on_left_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    ?assertEqual(on, merge(F1, F0)).
    
merge_on_right_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    ?assertEqual(on, merge(F0, F1)).
        
merge_on_both_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    ?assertEqual(on, merge(F1, F1)).

-endif.
