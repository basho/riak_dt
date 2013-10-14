%% -------------------------------------------------------------------
%%
%% riak_dt_od_flag: a flag that can be enabled and disabled as many
%%     times as you want, enabling wins, starts disabled.
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

-module(riak_dt_od_flag).

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

-export_type([od_flag/0, od_flag_op/0]).

-opaque od_flag() :: {riak_dt_vclock:vclock(), boolean()}.
-type od_flag_op() :: enable | disable.

-spec new() -> od_flag().
new() ->
    {riak_dt_vclock:fresh(), false}.

-spec value(od_flag()) -> boolean().
value({_, F}) -> F.

-spec value(term(), od_flag()) -> boolean().
value(_, Flag) ->
    value(Flag).

-spec update(od_flag_op(), riak_dt:actor(), od_flag()) -> {ok, od_flag()}.
update(enable, Actor, {Clock,_}=_Flag) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    {ok, {NewClock, true}};
update(disable, _Actor, {Clock,_}=_Flag) ->
    {ok, {Clock, false}}.

-spec merge(od_flag(), od_flag()) -> od_flag().
merge({C1, F}, {C2,F}) ->
    %% When they are the same result (true or false), just merge the
    %% vclock.
    {riak_dt_vclock:merge([C1, C2]), F};
merge({C1, _}=ODF1, {C2, _}=ODF2) ->
    %% When the flag disagrees:
    case {riak_dt_vclock:equal(C1, C2),
          riak_dt_vclock:descends(C1, C2),
          riak_dt_vclock:descends(C2, C1)} of
    %% 1) If the clocks are equal, the result is 'false' (observed
    %% disable).
        {true, _, _} ->
            {riak_dt_vclock:merge([C1, C2]), false};
    %% 2) If they are sibling/divergent clocks, the result is 'true'.
        {_, false, false} ->
            {riak_dt_vclock:merge([C1, C2]), true};
    %% 3) If one clock dominates the other, its value should be
    %% chosen.
        {_, true, false} ->
            ODF1;
        {_, false, true} ->
            ODF2
    end.

-spec equal(od_flag(), od_flag()) -> boolean().
equal({C1,F},{C2,F}) ->
    riak_dt_vclock:equal(C1,C2);
equal(_,_) -> false.

-define(TAG, 73).
-define(VSN1, 1).

-spec from_binary(binary()) -> od_flag().
from_binary(<<?TAG:8, ?VSN1:8, BFlag:8, VClock/binary>>) ->
    Flag = case BFlag of
               1 -> true;
               0 -> false
           end,
    {riak_dt_vclock:from_binary(VClock), Flag}.

-spec to_binary(od_flag()) -> binary().
to_binary({Clock, Flag}) ->
    BFlag = case Flag of
                true -> 1;
                false -> 0
            end,
    VCBin = riak_dt_vclock:to_binary(Clock),
    <<?TAG:8, ?VSN1:8, BFlag:8, VCBin/binary>>.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, ?NUMTESTS).

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
    orddict:store(ID, false, Dict);
update_expected(ID, enable, Dict) ->
    orddict:store(ID, true, Dict);
update_expected(ID, disable, Dict) ->
    orddict:store(ID, false, Dict);
update_expected(ID, {merge, SourceID}, Dict) ->
    Mine = orddict:fetch(ID, Dict),
    Theirs = orddict:fetch(SourceID, Dict),
    Merged = Mine or Theirs,
    orddict:store(ID, Merged, Dict).

eqc_state_value(Dict) ->
    orddict:fold(fun(_K, V, Acc) -> V or Acc end, false, Dict).
-endif.

new_test() ->
    ?assertEqual(false, value(new())).

update_enable_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(true, value(F1)).

update_enable_multi_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(disable, 1, F1),
    {ok, F3} = update(enable, 1, F2),
    ?assertEqual(true, value(F3)).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(false, value(merge(F0, F0))).

merge_simple_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    ?assertEqual(true, value(merge(F1, F0))),
    ?assertEqual(true, value(merge(F0, F1))),
    ?assertEqual(true, value(merge(F1, F1))).

merge_concurrent_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(disable, 1, F1),
    {ok, F3} = update(enable, 1, F1),
    ?assertEqual(true, value(merge(F1,F3))),
    ?assertEqual(false, value(merge(F1,F2))),
    ?assertEqual(true, value(merge(F2,F3))).

binary_roundtrip_test() ->
    F0 = new(),
    {ok, F1} = update(enable, 1, F0),
    {ok, F2} = update(disable, 1, F1),
    {ok, F3} = update(enable, 2, F2),
    ?assert(equal(from_binary(to_binary(F3)), F3)).
-endif.
