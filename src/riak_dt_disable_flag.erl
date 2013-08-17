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
-behaviour(riak_dt_gc).

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

-define(TAG, 80).

new() ->
    on.

value(Flag) ->
    Flag.

value(_, Flag) ->
    Flag.

update(disable, _Actor, _Flag) ->
    off.

merge(FA, FB) ->
    flag_and(FA, FB).

equal(FA,FB) ->
    FA =:= FB.

from_binary(<<?TAG:7, 0:1>>) -> off;
from_binary(<<?TAG:7, 1:1>>) -> on.

to_binary(off) -> <<?TAG:7, 0:1>>;
to_binary(on) -> <<?TAG:7, 1:1>>.

%% priv
flag_and(on, on) ->
    on;
flag_and(_, _) ->
    off.

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
    on.

gen_op() ->
    disable.
    
gen_gc_ops() ->
    [].

update_expected(_ID, disable, _Prev) ->
    off;
update_expected(_ID, _Op, Prev) ->
    Prev.

eqc_state_value(S) ->
    S.

gc_model_create() ->
    true.

gc_model_update(disable, _Actor, _Flag) ->
    false.

gc_model_merge(Flag1, Flag2) ->
    Flag1 and Flag2.

gc_model_realise(true) ->
    on;
gc_model_realise(false) ->
    off.

gc_model_ready(_Meta, _Flag) ->
    false.


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
