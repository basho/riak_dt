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

-export([new/0, value/1, value/2, update/3, merge/2, equal/2, from_binary/1, to_binary/1]).
-export([gc_epoch/1, gc_ready/2, gc_get_fragment/2, gc_replace_fragment/3]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, init_state/0, update_expected/3, eqc_state_value/1]).
-compile(export_all).

-behaviour(crdt_gc_statem_eqc).
-export([gen_gc_ops/0]).
-export([gc_model_create/0, gc_model_update/3, gc_model_merge/2, gc_model_realise/1]).
-export([gc_model_ready/2]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(TAG, 79).

new() ->
    off.

value(Flag) ->
    Flag.

value(_, Flag) ->
    Flag.

update(enable, _Actor, _Flag) ->
    on.

merge(FA, FB) ->
    flag_or(FA, FB).

equal(FA,FB) ->
    FA =:= FB.

from_binary(<<?TAG:7, 0:1>>) -> off;
from_binary(<<?TAG:7, 1:1>>) -> on.

to_binary(off) -> <<?TAG:7, 0:1>>;
to_binary(on) -> <<?TAG:7, 1:1>>.

%% priv
flag_or(off, off) ->
    off;
flag_or(_, _) ->
    on.

gc_epoch(_Flag) ->
    undefined.

gc_ready(_Meta, _Flag) ->
    false.

gc_get_fragment(_Meta, _Flag) ->
    {}.

gc_replace_fragment(_Meta, _Frag, Flag) ->
    Flag.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

eqc_gc_test_() ->
    crdt_gc_statem_eqc:run(?MODULE, 200).
    
%% EQC generator
init_state() -> 
    off.    

gen_op() ->
    enable.

gen_gc_ops() ->
    [].

update_expected(_ID, enable, _Prev) ->
    on;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.

gc_model_create() ->
    false.

gc_model_update(enable, _Actor, _Flag) ->
    true.

gc_model_merge(Flag1, Flag2) ->
    Flag1 or Flag2.

gc_model_realise(true) ->
    on;
gc_model_realise(false) ->
    off.

gc_model_ready(_Meta, _Flag) ->
    false.
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
