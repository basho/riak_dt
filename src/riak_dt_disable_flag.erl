%% -------------------------------------------------------------------
%%
%% riak_dt_disable_flag: A state based, disable-only flag.
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
%%% an ON/OFF (boolean) CRDT, which converges towards OFF
%%% it has a single operation: disable.
%%% @end

-module(riak_dt_disable_flag).

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
    disable.

update_expected(_ID, disable, _Prev) ->
    off;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.
-endif.

new() ->
    on.

value(Flag) ->
    Flag.

update(disable, _Actor, _Flag) ->
    off.

merge(FA, FB) ->
    flag_and(FA, FB).

equal(FA,FB) ->
    FA =:= FB.

%% priv
flag_and(on, on) ->
    on;
flag_and(off, _) ->
    off;
flag_and(_, off) ->
    off.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(on, 1000, ?MODULE))]}.
-endif.

new_test() ->
    ?assertEqual(on, new()).

update_enable_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    ?assertEqual(off, F1).

update_enable_multi_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    F2 = update(disable, 1, F1),
    ?assertEqual(off, F2).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(on, merge(F0, F0)).

merge_on_left_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    ?assertEqual(off, merge(F1, F0)).
    
merge_on_right_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    ?assertEqual(off, merge(F0, F1)).
        
merge_on_both_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    ?assertEqual(off, merge(F1, F1)).

-endif.
