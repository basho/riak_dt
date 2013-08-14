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

-export([new/0, value/1, value/2, update/3, merge/2, equal/2, from_binary/1, to_binary/1]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, init_state/0, update_expected/3, eqc_state_value/1]).
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% EQC generator
-ifdef(EQC).
gen_op() ->
    oneof([disable,enable]).

init_state() ->
    orddict:new().

update_expected(ID, create, Dict) ->
    orddict:store(ID, off, Dict);
update_expected(ID, enable, Dict) ->
    orddict:store(ID, on, Dict);
update_expected(ID, disable, Dict) ->
    orddict:store(ID, off, Dict);
update_expected(ID, {merge, SourceID}, Dict) ->
    Mine = orddict:fetch(ID, Dict),
    Theirs = orddict:fetch(SourceID, Dict),
    Merged = flag_or(Mine,Theirs),
    orddict:store(ID, Merged, Dict).

flag_or(off, off) ->
    off;
flag_or(_, _) ->
    on.

eqc_state_value(Dict) ->
    orddict:fold(fun(_K,V,Acc) ->
            flag_or(V,Acc)
        end, off, Dict).
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
