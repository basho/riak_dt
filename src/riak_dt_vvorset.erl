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
-behaviour(riak_dt_gc).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, update/3, merge/2, equal/2]).
-export([gc_epoch/1, gc_ready/2, gc_get_fragment/2, gc_replace_fragment/3]).

-include("riak_dt_gc_meta.hrl").
-opaque vvorset() :: {orddict:orddict(), orddict:orddict()}.

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1, init_state/0]).
-endif.

new() ->
    {orddict:new(), orddict:new()}.

value({ADict, RDict}) ->
    orddict:fetch_keys(orddict:filter(fun(K, AClock) ->
                                        RClock = vclock_for(K, RDict),
                                        not vclock:descends(RClock, AClock)
                                end,
                                ADict)).

update({add, Elem}, Actor, {ADict0, RDict}) ->
    ADict = add_elem(Actor, ADict0, Elem),
    {ADict, RDict};
update({remove, Elem}, _Actor, {ADict, RDict0}) ->
    RDict = remove_elem(orddict:find(Elem, ADict), Elem, RDict0),
    {ADict, RDict}.
merge({ADict1, RDict1}, {ADict2, RDict2}) ->
    MergedADict = merge_dicts(ADict1, ADict2),
    MergedRDict = merge_dicts(RDict1, RDict2),
    {MergedADict, MergedRDict}.

equal({ADict1, RDict1}, {ADict2, RDict2}) ->
    ADict1 == ADict2 andalso RDict1 == RDict2.

%%% GC

-type gc_fragment() :: vvorset().

% We want to gc if Tombstone Elements make up more than `compact_proportion` of the elements in the vvorset.
-spec gc_ready(gc_meta(), vvorset()) -> boolean().
gc_ready(Meta, {Add,_Remove}=VVORSet) ->
    TombstoneCount = orddict:size(tombstones(VVORSet)),
    ElementCount   = orddict:size(Add),
    (TombstoneCount / ElementCount) > Meta?GC_META.compact_proportion.

% I don't know how we find the epoch
-spec gc_epoch(vvorset()) -> epoch().
gc_epoch(_ORSet) ->
    {}.

% Just two copies of all Elements that were present but are no longer, complete
% with their vclocks.
-spec gc_get_fragment(gc_meta(), vvorset()) -> gc_fragment().
gc_get_fragment(_Meta, VVORSet) ->
    Tombstones = tombstones(VVORSet),
    {Tombstones,Tombstones}.

% Remove all tombstones, but only if the vclock in the fragment is not 
% dominated by the vclock in the vvorset (the if dominated, suggests updates 
% have happened since)
-spec gc_replace_fragment(gc_meta(), gc_fragment(), vvorset()) -> vvorset().
gc_replace_fragment(_Meta, {AddFrag,RemFrag}=_VVORFrag, {Add0,Rem0}=_VVORSet) ->
    Add1 = orddict:filter(remove_fragment_filter(AddFrag), Add0),
    Rem1 = orddict:filter(remove_fragment_filter(RemFrag), Rem0),
    {Add1,Rem1}.

%% Private
add_elem(Actor, Dict, Elem) ->
    VC = vclock:fresh(),
    InitialValue = vclock:increment(Actor, VC),
    orddict:update(Elem, update_fun(Actor), InitialValue, Dict).

update_fun(Actor) ->
    fun(Vclock) ->
        vclock:increment(Actor, Vclock)
    end.

remove_elem({ok, Aclock}, Elem, RDict) ->
    Rclock = vclock_for(Elem, RDict),
    orddict:store(Elem, vclock:merge([Aclock,Rclock]), RDict);
remove_elem(error, _Elem, RDict) ->
    %% What @TODO?
    %% Can't remove an element not in the ADict, warn??
    %% Should we add to the remove set with a fresh + actor+1 vclock?
    %% Throw an error? (seems best)
    RDict.

merge_dicts(Dict1, Dict2) ->
    %% for every key in dict1, merge its contents with dict2's content for same key
   orddict:merge(fun(_K, V1, V2) -> vclock:merge([V1, V2]) end, Dict1, Dict2).

vclock_for(Elem, Dict) ->
    case orddict:find(Elem, Dict) of
        {ok, Clock} -> Clock;
        error       -> vclock:fresh()
    end.

tombstones({ADict,RDict}) ->
    orddict:filter(fun(K,AClock) ->
            vclock:descends(vclock_for(K,RDict),AClock)
        end, ADict).
        
remove_fragment_filter(Fragment) ->
    fun (Elem,VClock) ->
        FragVClock = vclock_for(Elem,Fragment),
        not vclock:descends(FragVClock,VClock)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
-compile(export_all).
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

create_gc_expected() ->
    {ordsets:new(), ordsets:new()}.

update_gc_expected({add, X}, Actor, {Add,Remove}) ->
    Unique = erlang:phash2({Actor, erlang:now()}),
    {ordsets:add_element({X, Unique}, Add), ordsets:del_element(X,Remove)};
update_gc_expected({remove, X}, _Actor, {Add,Remove}) ->
    ToRem = [{A,Elem} || {A,Elem} <- ordsets:to_list(Add), Elem == X],
    Remove1 = ordsets:union(ordsets:from_list(ToRem),Remove),
    {Add, Remove1}.
% update_gc_expected(_Operation, _Actor, State) ->
%     State.

merge_gc_expected({A1,R1}, {A2,R2}) ->
    A3 = ordsets:union(A1,A2),
    R3 = ordsets:union(R1,R2),
    {A3,R3}.

realise_gc_expected({Add,Remove}) ->
    Values = [ X || {X, _A} <- ordsets:to_list(ordsets:subtract(Add,Remove))],
    lists:usort(Values).

-endif.
-endif.
