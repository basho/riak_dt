%% -------------------------------------------------------------------
%%
%% riak_dt_oe_flag: a flag that can be enabled and disabled as many times as you want, disabling wins, starts enabled.
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

-export([new/0, value/1, update/3, merge/2, equal/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
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

% {Enables,Disables}
new() ->
    {ordsets:new(),ordsets:new()}.

value({Disables,Enables}=_Flag) ->
    Winners = ordsets:subtract(Disables,Enables),
    case ordsets:size(Winners) of
        0 -> on;
        _ -> off
    end.

update(disable, Actor, {Disables,Enables}=_Flag) ->
    Token = unique_token(Actor),
    Disables1 = ordsets:add_element(Token,Disables),
    {Disables1, Enables};
update(enable, _Actor, {Disables,Enables}=_Flag) ->
    {Disables,ordsets:union(Disables,Enables)}.

merge({DA,EA}=_FA, {DB,EB}=_FB) ->
    Disables = ordsets:union(DA,DB),
    Enables = ordsets:union(EA,EB),
    {Disables, Enables}.

equal(FlagA,FlagB) ->
    FlagA == FlagB.

%% priv
unique_token(Actor) ->
    erlang:phash2({Actor, erlang:now()}).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 1200, [?_assert(crdt_statem_eqc:prop_converge(init_state(), 10000, ?MODULE))]}.
-endif.

new_test() ->
    ?assertEqual(on, value(new())).

update_enable_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    ?assertEqual(off, value(F1)).

update_enable_multi_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    F2 = update(enable, 1, F1),
    F3 = update(disable, 1, F2),
    ?assertEqual(off, value(F3)).

merge_offs_test() ->
    F0 = new(),
    ?assertEqual(on, value(merge(F0, F0))).

merge_simple_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    ?assertEqual(off, value(merge(F1, F0))),
    ?assertEqual(off, value(merge(F0, F1))),
    ?assertEqual(off, value(merge(F1, F1))).

merge_concurrent_test() ->
    F0 = new(),
    F1 = update(disable, 1, F0),
    F2 = update(enable, 1, F1),
    F3 = update(disable, 1, F1),
    ?assertEqual(off, value(merge(F1,F3))),
    ?assertEqual(on, value(merge(F1,F2))),
    ?assertEqual(off, value(merge(F2,F3))).

-endif.
