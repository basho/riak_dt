%% -------------------------------------------------------------------
%%
%% riak_dt_orset: A convergent, replicated, state based observe remove set
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_dt_orset).

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
-opaque orset() :: {orddict:orddict(), orddict:orddict()}.

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1]).
-compile(export_all).
-endif.

%% EQC generator
-ifdef(EQC).
gen_op() ->
    ?LET({Add, Remove}, gen_elems(),
         oneof([{add, Add}, {remove, Remove}])).

gen_elems() ->
    ?LET(A, int(), {A, oneof([A, int()])}).

%% Maybe model qc state as op based?
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

%% GC Property

create_gc_expected() ->
    {ordsets:new(), ordsets:new()}.

update_gc_expected({add, Elem}, Actor, {Add,Remove}) ->
    Unique = erlang:phash2({Actor, erlang:now()}),
    {ordsets:add_element({Elem, Unique}, Add), Remove};
update_gc_expected({remove, RemElem}, _Actor, {Add,Remove}) ->
    ToRem = [{Elem,A} || {Elem,A} <- ordsets:to_list(Add), Elem == RemElem],
    Remove1 = ordsets:union(ordsets:from_list(ToRem),Remove),
    {Add, Remove1}.

merge_gc_expected({A1,R1}, {A2,R2}) ->
    A3 = ordsets:union(A1,A2),
    R3 = ordsets:union(R1,R2),
    {A3,R3}.

realise_gc_expected({Add,Remove}) ->
    Values = [ Elem || {Elem, _A} <- ordsets:to_list(ordsets:subtract(Add,Remove))],
    lists:usort(Values).
    
eqc_gc_ready(Meta, {Add,Remove}) ->
    TotalTokens = ordsets:size(ordsets:union(Add,Remove)),
    TombstoneTokens = ordsets:size(ordsets:intersection(Add,Remove)),
    case TotalTokens of
        0 -> false;
        _ -> (TombstoneTokens / TotalTokens) > Meta?GC_META.compact_proportion
    end.

eqc_gc_get_fragment(_Meta, _S) ->
    {}.

-endif.

new() ->
    {orddict:new(), orddict:new()}.

value({ADict, RDict}) ->
    orddict:fetch_keys(orddict:filter(fun(K, V) ->
                                        case orddict:find(K, RDict) of
                                            {ok, RSet} ->
                                                case
                                                    ordsets:to_list(ordsets:subtract(V, RSet)) of
                                                    [] -> false;
                                                    _ -> true
                                                end;
                                            error -> true
                                        end
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

-type gc_fragment() :: orset().

% We're Ready to GC if Tombstones make up more than compact_proportion of the total number of tokens.
-spec gc_ready(gc_meta(), orset()) -> boolean().
gc_ready(Meta, {Add,Remove}=_ORSet) ->
    % These folds add {elem, token} to a set, on the off-chance that two tokens
    % for seperate keys are the same.
    Additions = orddict:fold(fun ordsets_union_prefix/3, ordsets:new(),Add),
    Removals = orddict:fold(fun ordsets_union_prefix/3, ordsets:new(),Remove),
    TotalTokens = ordsets:size(ordsets:union(Additions,Removals)),
    TombstoneTokens = ordsets:size(ordsets:intersection(Additions,Removals)),
    case TotalTokens of
        0 -> false;
        _ -> (TombstoneTokens / TotalTokens) > Meta?GC_META.compact_proportion
    end.

% Gulp, I literally have no idea, one reason is we don't embed the epoch in 
-spec gc_epoch(orset()) -> epoch().
gc_epoch(_ORSet) ->
    {}.

% The fragment is just an ORSet with all invalidated tokens removed (but using 
% the same tokens as the original ORSet).
-spec gc_get_fragment(gc_meta(), orset()) -> gc_fragment().
gc_get_fragment(_Meta, {Add,Rem}=_ORSet) ->
    Additions = orddict:fold(fun ordsets_union_prefix/3, ordsets:new(), Add),
    Removals = orddict:fold(fun ordsets_union_prefix/3,  ordsets:new(), Rem),
    TombstoneTokens = ordsets:intersection(Additions,Removals),
    ordsets:fold(fun({Elem,Token},{Add0,Rem0}) ->
                    % For add we need to bypass token generation. Le Sigh
                    Add1 = add_unique(orddict:find(Elem,Add0), Add0, Elem, Token),
                    Rem1 = remove_elem(orddict:find(Elem,Add1), Elem, Rem0),
                    {Add1, Rem1}
                 end, new(), TombstoneTokens).

% Now we go through the orset removing all tokens present in the fragment.
-spec gc_replace_fragment(gc_meta(), gc_fragment(), orset()) -> orset().
gc_replace_fragment(_Meta, {AddFrag,RemFrag}=_ORFrag, {Add0,Rem0}=_ORSet) ->
    Add1 = orddict:merge(fun ordsets_subtract/3, Add0, AddFrag),
    Rem1 = orddict:merge(fun ordsets_subtract/3, Rem0, RemFrag),
    {filter_empty(Add1), filter_empty(Rem1)}.

%% Private
add_elem(Actor, Dict, Elem) ->
    Unique = unique(Actor),
    add_unique(orddict:find(Elem, Dict), Dict, Elem, Unique).

remove_elem({ok, Set0}, Elem, RDict) ->
    case orddict:find(Elem, RDict) of
        {ok, Set} ->
            orddict:store(Elem, ordsets:union(Set, Set0), RDict);
        error ->
            orddict:store(Elem, Set0, RDict)
    end;
remove_elem(error, _Elem, RDict) ->
    %% Can't remove an element not in the ADict, warn??
    RDict.

add_unique({ok, Set0}, Dict, Elem, Unique) ->
    Set = ordsets:add_element(Unique, Set0),
    orddict:store(Elem, Set, Dict);
add_unique(error, Dict, Elem, Unique) ->
    Set = ordsets:from_list([Unique]),
    orddict:store(Elem, Set, Dict).

unique(Actor) ->
    erlang:phash2({Actor, erlang:now()}).

merge_dicts(Dict1, Dict2) ->
    %% for every key in dict1, merge its contents with dict2's content for same key
   orddict:merge(fun ordsets_union/3, Dict1, Dict2).

ordsets_union(_K, V1, V2) ->
    ordsets:union(V1, V2).

ordsets_union_prefix(K, V, KTokenSet) ->
    KTs = ordsets:from_list([{K,Token} || Token <- V]),
    ordsets:union(KTs, KTokenSet).

% Subtracts the elements in V2 from V1
ordsets_subtract(_K, V1, V2) ->
    ordsets:subtract(V1, V2).

filter_empty(OrdDict) ->
    orddict:filter(fun(_K,[]) -> false;
                      (_K,_V) -> true
                  end, OrdDict).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    {timeout, 120, [?_assert(crdt_statem_eqc:prop_converge(init_state(), 1000, ?MODULE))]}.
-endif.
-endif.
