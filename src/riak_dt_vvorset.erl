%% -------------------------------------------------------------------
%%
%% riak_dt_vvorset: Another convergent, replicated, state based observe remove set
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

-module(riak_dt_vvorset).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, update/3, merge/2, equal/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0]).
-endif.

new() ->
    %% Use a list of actors, and represent the actors as ints
    %% a sort of compression
    {[], orddict:new()}.

value({_AL, ORSet}) ->
    [K || {K, {Active, _Vclock}} <- orddict:to_list(ORSet), Active == 1].

update({add, Elem}, Actor, ORSet) ->
    add_elem(Actor, ORSet, Elem);
update({remove, Elem}, _Actor, ORSet) ->
    remove_elem(orddict:find(Elem, ORSet), Elem, ORSet).

merge(ORSet1, ORSet2) ->
    merge_dicts(ORSet1, ORSet2).

equal(ORSet1, ORSet2) ->
    ORSet1 == ORSet2.

%% Private
add_elem(Actor, {AL, Dict}, Elem) ->
    {AL1, Pos} = actor_pos(AL, Actor, 1, []),
    InitialValue = {1, vclock:increment(Pos, vclock:fresh())},
    {AL1, orddict:update(Elem, update_fun(Pos), InitialValue, Dict)}.

update_fun(Actor) ->
    fun({_, Vclock}) ->
            {1, vclock:increment(Actor, Vclock)}
    end.

%% Get the index of the actor in the actor list
actor_pos([], Actor, Elem, AL) ->
    {AL ++ [Actor], Elem};
actor_pos([Actor | _Rest], Actor, Elem, AL) ->
    {AL, Elem};
actor_pos([_NA | Rest], Actor, Elem, AL) ->
    actor_pos(Rest, Actor, Elem+1, AL).

remove_elem({ok, {1, Vclock}}, Elem, {AL, Dict}) ->
    {AL, orddict:store(Elem, {0, Vclock}, Dict)};
remove_elem(_, _Elem, ORSet) ->
    %% What @TODO?
    %% Can't remove an element not in the ADict, warn??
    %% Should we add to the remove set with a fresh + actor+1 vclock?
    %% Throw an error? (seems best)
    ORSet.

merge_dicts(Dict1, Dict2) ->
    %% for every key in dict1, merge its contents with dict2's content for same key
    %% if the keys values are both active or both deleted, simply merge the clocks
    %% if one is active and one is deleted, check that the deleted dominates the added
    %% if so, set to that value and deleted, otherwise keep as is (no need to merge vclocks?)
    orddict:merge(fun(_K, {Bool, V1}, {Bool, V2}) -> {Bool, vclock:merge([V1, V2])};
                     (_K, {0, V1}, {1, V2}) -> is_active_or_removed(V1, V2);
                     (_K, {1, V1}, {0, V2}) -> is_active_or_removed(V2, V1) end,
                     Dict1, Dict2).
%% @Doc determine if the entry is active or removed.
%% First argument is a remove vclock, second is an active vclock
is_active_or_removed(RemoveClock, AddClock) ->
    case (not vclock:descends(RemoveClock, AddClock)) of
        true ->
            {1, AddClock};
        false ->
            {0, RemoveClock}
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(init_state(), 1000, ?MODULE))]}.

%% EQC generator
gen_op() ->
    ?LET({Add, Remove}, gen_elems(),
         oneof([{add, Add}, {remove, Remove}])).

gen_elems() ->
    ?LET(A, int(), {A, oneof([A, int()])}).

init_state() ->
    {0, dict:new(), []}.

update_expected(ID, {add, Elem}, {Cnt0, Dict, L}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict), [{ID, {add, Elem}}|L]};
update_expected(ID, {remove, Elem}, {Cnt, Dict, L}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict), [{ID, {remove, Elem, ToRem}}|L]};
update_expected(ID, {merge, SourceID}, {Cnt, Dict, L}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict), [{ID,{merge, SourceID}}|L]};
update_expected(ID, create, {Cnt, Dict, L}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict), [{ID, create}|L]}.

eqc_state_value({_Cnt, Dict, _L}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Values = [ Elem || {Elem, _X} <- sets:to_list(Remaining)],
    lists:usort(Values).

-endif.
-endif.
