%% -------------------------------------------------------------------
%%
%% riak_dt_od_flag: a flag that can be enabled and disabled as many times as you want, enabling wins, starts disabled.
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
-behaviour(riak_dt_gc).

-export([new/0, value/1, value/2, update/3, merge/2, equal/2, from_binary/1, to_binary/1]).
-export([gc_epoch/1, gc_ready/2, gc_get_fragment/2, gc_replace_fragment/3]).

-include("riak_dt_gc_meta.hrl").

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

% {Enables,Disables}
new() ->
    {ordsets:new(),ordsets:new()}.

value({Enables,Disables}=_Flag) ->
    Winners = ordsets:subtract(Enables,Disables),
    case ordsets:size(Winners) of
        0 -> off;
        _ -> on
    end.

value(_, Flag) ->
    value(Flag).

update(enable, Actor, {Enables,Disables}=_Flag) ->
    Token = unique_token(Actor),
    Enables1 = ordsets:add_element(Token,Enables),
    {Enables1, Disables};
update(disable, _Actor, {Enables,Disables}=_Flag) ->
    {Enables,ordsets:union(Enables,Disables)}.

merge({EA,DA}=_FA, {EB,DB}=_FB) ->
    Enables = ordsets:union(EA,EB),
    Disables = ordsets:union(DA,DB),
    {Enables, Disables}.

equal(FlagA,FlagB) ->
    FlagA == FlagB.

-define(TAG, 73).
-define(VSN1, 1).

from_binary(<<?TAG:8, ?VSN1:8, ESize:32, DSize:32,
              EBin:ESize/binary, DBin:DSize/binary>>) ->
    Enables = ordsets:from_list([ E || <<E:32>> <= EBin ]),
    Disables = ordsets:from_list([ D || <<D:32>> <= DBin ]),
    {Enables, Disables}.

to_binary({Enables, Disables}) ->
    ESize = ordsets:size(Enables) * 4,
    DSize = ordsets:size(Disables) * 4,
    EBin = << <<T:32>> || T <- ordsets:to_list(Enables) >>,
    DBin = << <<T:32>> || T <- ordsets:to_list(Disables) >>,
    <<?TAG:8, ?VSN1:8, ESize:32, DSize:32, EBin/binary, DBin/binary>>.

% TODO
gc_epoch(_ODFlag) ->
    undefined.

gc_ready(Meta, {Enables,Disables}) ->
    TotalSize = ordsets:size(ordsets:union(Enables,Disables)),
    KeepSize = ordsets:size(ordsets:subtract(Enables,Disables)),
    case TotalSize of
        0 -> false;
        _ -> ?SHOULD_GC(Meta, KeepSize/TotalSize)
    end.

gc_get_fragment(_Meta, {Enables,Disables}) ->
    Tombstones = ordsets:intersection(Enables,Disables),
    {Tombstones,Tombstones}.

gc_replace_fragment(_Meta, {EnFrag,DisFrag}, {Enables,Disables}) ->
    {ordsets:subtract(Enables,EnFrag), ordsets:subtract(Disables,DisFrag)}.

%% priv
unique_token(Actor) ->
    erlang:phash2({Actor, erlang:now()}).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

% TODO, this fails intermittently. Need to refine the model again methinks
eqc_gc_test_() ->
    crdt_gc_statem_eqc:run(?MODULE, 200).

% EQC Generator
gen_op() ->
    oneof([disable,enable]).
    
gen_gc_ops() ->
    [enable,disable].

init_state() ->
    orddict:new().

update_expected(ID, create, Dict) ->
    orddict:store(ID, false, Dict);
update_expected(ID, enable, Dict) ->
    orddict:store(ID, true, Dict);
update_expected(ID, disable, Dict) ->
    orddict:store(ID, false, Dict);
update_expected(ID, {merge, SourceID}, Dict) ->
    Mine = orddict:fetch(ID, Dict),
    Theirs = orddict:fetch(SourceID, Dict),
    orddict:store(ID, Mine or Theirs, Dict).

eqc_state_value(Dict) ->
    B = orddict:fold(fun(_K,V,Acc) ->
            V or Acc
        end, false, Dict),
    case B of
        true -> on;
        false -> off
    end.

% This model is fun. Essentially we keep track of all updates, plus their concurrency. Using a history of {ts,value} pairs, and then a seperate logical timestamp, and then counts for numbers of updates vs number of deletes
gc_model_create() ->
    {orddict:new(), 0, riak_dt_gcounter:new(), riak_dt_gcounter:new()}.

gc_model_update(enable, Actor, {Dict,Ts,Total,Disables}) ->
    Ts1 = Ts+1,
    Total1 = riak_dt_gcounter:update(increment, Actor, Total),
    {orddict:store(Ts1, true, Dict), Ts1, Total1, Disables};
gc_model_update(disable, _Actor, {Dict, Ts, Total, Disables}) ->
    Ts1 = Ts+1,
    Disables1 = riak_dt_gcounter:merge(Total, Disables),
    {orddict:store(Ts1, false, Dict), Ts1, Total, Disables1}.

% The merge fun has to do *something*, not that we should ever hit it.
gc_model_merge({Dict1,Ts1,Tot1,Dis1}, {Dict2,Ts2,Tot2,Dis2}) ->
    Dict = orddict:merge(fun(_K,B1,B2) ->
            B1 or B2 % Concurrent updates, `true` wins.
        end, Dict1, Dict2),
    Tot = riak_dt_gcounter:merge(Tot1,Tot2),
    Dis = riak_dt_gcounter:merge(Dis1,Dis2),
    {Dict, max(Ts1, Ts2), Tot, Dis}.

gc_model_realise({_, 0, _, _}) ->
    off;
gc_model_realise({Dict, Ts, _, _}) ->
    {ok, Value} = orddict:find(Ts, Dict),
    case Value of
        true -> on;
        false -> off
    end.

% TODO: this should still fail, as it doesn't cope with GC happening at all (yet). I have *no* idea how to solve that case.
% Does this suggest we need a model post-gc hook of some kind?
gc_model_ready(Meta, {_,_,Total,Disables}) ->
    TotalCnt = riak_dt_gcounter:value(Total),
    DisableCnt = riak_dt_gcounter:value(Disables),
    case TotalCnt of
        0 -> false;
        _ -> ?SHOULD_GC(Meta, 1 - (DisableCnt/TotalCnt))
    end.
-endif.

new_test() ->
    ?assertEqual(off, value(new())).

update_enable_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    ?assertEqual(on, value(F1)).

update_enable_multi_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    F2 = update(disable, 1, F1),
    F3 = update(enable, 1, F2),
    ?assertEqual(on, value(F3)).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(off, value(merge(F0, F0))).

merge_simple_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    ?assertEqual(on, value(merge(F1, F0))),
    ?assertEqual(on, value(merge(F0, F1))),
    ?assertEqual(on, value(merge(F1, F1))).

merge_concurrent_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    F2 = update(disable, 1, F1),
    F3 = update(enable, 1, F1),
    ?assertEqual(on, value(merge(F1,F3))),
    ?assertEqual(off, value(merge(F1,F2))),
    ?assertEqual(on, value(merge(F2,F3))).

binary_roundtrip_test() ->
    F0 = new(),
    F1 = update(enable, 1, F0),
    F2 = update(disable, 1, F1),
    F3 = update(enable, 2, F2),
    ?assert(equal(from_binary(to_binary(F3)), F3)).

-endif.
