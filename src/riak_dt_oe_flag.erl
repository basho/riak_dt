%% -------------------------------------------------------------------
%%
%% riak_dt_oe_flag: a flag that can be enabled and disabled as many
%%   times as you want, disabling wins, starts enabled.
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

-module(riak_dt_oe_flag).

-behaviour(riak_dt).

-export([new/0, value/1, value/2, update/3, merge/2, equal/2, from_binary/1, to_binary/1]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, init_state/0, update_expected/3, eqc_state_value/1, generate/0, size/1]).
-define(NUMTESTS, 1000).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([oe_flag/0, oe_flag_op/0]).

-opaque oe_flag() :: {riak_dt_vclock:vclock(), on | off}.
-type oe_flag_op() :: enable | disable.

-spec new() -> oe_flag().
new() ->
    {riak_dt_vclock:fresh(), on}.

-spec value(oe_flag()) -> on | off.
value({_,F}) -> F.

-spec value(term(), oe_flag()) -> on | off.
value(_, Flag) ->
    value(Flag).

-spec update(oe_flag_op(), riak_dt:actor(), oe_flag()) -> {ok, oe_flag()}.
update(disable, Actor, {Clock,_}) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    {ok, {NewClock, off}};
update(enable, _Actor, {Clock,_}) ->
    {ok, {Clock, on}}.

-spec merge(oe_flag(), oe_flag()) -> oe_flag().
merge({C1, F}, {C2,F}) ->
    %% When they are the same result (on or off), just merge the
    %% vclock.
    {riak_dt_vclock:merge([C1, C2]), F};
merge({C1, _}=ODF1, {C2, _}=ODF2) ->
    %% When the flag disagrees:
    case {riak_dt_vclock:equal(C1, C2),
          riak_dt_vclock:descends(C1, C2),
          riak_dt_vclock:descends(C2, C1)} of
    %% 1) If the clocks are equal, the result is 'on' (observed
    %% enable).
        {true, _, _} ->
            {riak_dt_vclock:merge([C1, C2]), on};
    %% 2) If they are sibling/divergent clocks, the result is 'off'.
        {_, false, false} ->
            {riak_dt_vclock:merge([C1, C2]), off};
    %% 3) If one clock dominates the other, its value should be
    %% chosen.
        {_, true, false} ->
            ODF1;
        {_, false, true} ->
            ODF2
    end.

-spec equal(oe_flag(), oe_flag()) -> boolean().
equal({C1,F},{C2,F}) ->
    riak_dt_vclock:equal(C1,C2);
equal(_,_) -> false.

-define(TAG, 74).
-define(VSN1, 1).

-spec from_binary(binary()) -> oe_flag().
from_binary(<<?TAG:8, ?VSN1:8, BFlag:8, VClock/binary>>) ->
    Flag = case BFlag of
               1 -> on;
               0 -> off
           end,
    {riak_dt_vclock:from_binary(VClock), Flag}.

-spec to_binary(oe_flag()) -> binary().
to_binary({Clock, Flag}) ->
    BFlag = case Flag of
                on -> 1;
                off -> 0
            end,
    VCBin = riak_dt_vclock:to_binary(Clock),
    <<?TAG:8, ?VSN1:8, BFlag:8, VCBin/binary>>.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

bin_roundtrip_test_() ->
    crdt_statem_eqc:run_binary_rt(?MODULE, ?NUMTESTS).

% EQC generator
gen_op() ->
    oneof([disable,enable]).

size({Clock,_}) ->
    length(Clock).

init_state() ->
    orddict:new().

generate() ->
    ?LET(Ops, non_empty(list({gen_op(), binary(16)})),
         lists:foldl(fun({Op, Actor}, Flag) ->
                             {ok, F} = ?MODULE:update(Op, Actor, Flag),
                             F
                     end,
                     ?MODULE:new(),
                     Ops)).

update_expected(ID, create, Dict) ->
    orddict:store(ID, on, Dict);
update_expected(ID, enable, Dict) ->
    orddict:store(ID, on, Dict);
update_expected(ID, disable, Dict) ->
    orddict:store(ID, off, Dict);
update_expected(ID, {merge, SourceID}, Dict) ->
    Mine = orddict:fetch(ID, Dict),
    Theirs = orddict:fetch(SourceID, Dict),
    Merged = flag_and(Mine,Theirs),
    orddict:store(ID, Merged, Dict).

flag_and(on, on) ->
    on;
flag_and(_, _) ->
    off.

eqc_state_value(Dict) ->
    orddict:fold(fun(_K,V,Acc) ->
            flag_and(V,Acc)
        end, on, Dict).
-endif.

new_test() ->
    ?assertEqual(on, value(new())).

update_enable_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(off, value(F1)).

update_enable_multi_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    {ok, F2} = update(enable, 1, F1),
    {ok, F3} = update(disable, 1, F2),
    ?assertEqual(off, value(F3)).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(on, value(merge(F0, F0))).

merge_simple_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    ?assertEqual(off, value(merge(F1, F0))),
    ?assertEqual(off, value(merge(F0, F1))),
    ?assertEqual(off, value(merge(F1, F1))).

merge_concurrent_test() ->
    F0 = new(),
    {ok, F1} = update(disable, 1, F0),
    {ok, F2} = update(enable, 1, F1),
    {ok, F3} = update(disable, 1, F1),
    ?assertEqual(off, value(merge(F1,F3))),
    ?assertEqual(on, value(merge(F1,F2))),
    ?assertEqual(off, value(merge(F2,F3))).

-endif.
