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

%% API
-export([new/0, value/1, update/3, merge/2, equal/2,
         to_binary/1, from_binary/1, value/2, precondition_context/1]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% EQC API
-ifdef(EQC).
-export([init_state/0, gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-export_type([orset/0, binary_orset/0]).
-opaque orset() :: {entries(), entries()}.

-opaque binary_orset() :: binary(). %% A binary that from_binary/1 will operate on.

-type orset_op() :: {add, member()} | {remove, member()} |
                    {add_all, [member()]} | {remove_all, [member()]} |
                    {update, [orset_op()]}.

-type actor() :: riak_dt:actor().

%% an orddict
-type entries() :: [{member(), uniques()}].

%% an ordset
-type uniques() :: [unique()].
%% 0..2^27-1
%% @see phash/2
-type unique() :: non_neg_integer().
-type member() :: term().

-spec new() -> orset().
new() ->
    {orddict:new(), orddict:new()}.

-spec value(orset()) -> [member()].
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

%% @Doc note: not implemented yet, same as `value/1'
-spec value(any(), orset()) -> [member()].
value(_, ORSet) ->
    value(ORSet).


-spec update(orset_op(), actor(), orset()) -> orset().
update({update, Ops}, Actor, ORSet) ->
    apply_ops(lists:sort(Ops), Actor, ORSet);
update({add, Elem}, Actor, {ADict0, RDict}) ->
    ADict = add_elem(Actor, ADict0, Elem),
    {ok, {ADict, RDict}};
update({remove, Elem}, _Actor, {ADict, RDict0}) ->
    case remove_elem(orddict:find(Elem, ADict), Elem, RDict0) of
        {error, _}=Error ->
            Error;
        RDict ->
            {ok, {ADict, RDict}}
    end;
update({add_all, Elems}, Actor, {ADict, RDict}) ->
    ADict2 = lists:foldl(fun(E, S) ->
                                 add_elem(Actor, S, E) end,
                         ADict,
                         Elems),
    {ok, {ADict2, RDict}};
%% @Doc note: this is atomic, either _all_ `Elems` are removed, or
%% none are.
update({remove_all, Elems}, Actor, ORSet) ->
    remove_all(Elems, Actor, ORSet).

remove_all([], _Actor, ORSet) ->
    {ok, ORSet};
remove_all([Elem | Rest], Actor, ORSet) ->
    case update({remove, Elem}, Actor, ORSet) of
        {ok, ORSet2} ->
            remove_all(Rest, Actor, ORSet2);
        Error ->
            Error
    end.

apply_ops([], _Actor, ORSet) ->
    {ok, ORSet};
apply_ops([Op | Rest], Actor, ORSet) ->
    case update(Op, Actor, ORSet) of
        {ok, ORSet2} ->
            apply_ops(Rest, Actor, ORSet2);
        Error ->
            Error
    end.

-spec merge(orset(), orset()) -> orset().
merge({ADict1, RDict1}, {ADict2, RDict2}) ->
    MergedADict = merge_dicts(ADict1, ADict2),
    MergedRDict = merge_dicts(RDict1, RDict2),
    {MergedADict, MergedRDict}.

-spec equal(orset(), orset()) -> boolean().
equal({ADict1, RDict1}, {ADict2, RDict2}) ->
    ADict1 == ADict2 andalso RDict1 == RDict2.

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
remove_elem(error, Elem, _RDict) ->
    %% Can't remove an element not in the ADict
    {error, {precondition, {not_present, Elem}}}.

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
   orddict:merge(fun(_K, V1, V2) -> ordsets:union(V1, V2) end, Dict1, Dict2).

%% @doc the precondition context is a binary representation of a fragment of the CRDT
%% that operations with pre-conditions can be applied too.
%% In the case of OR-Sets this is the set of adds observed.
%% The system can then apply a remove to this context and merge it with a replica.
%% Especially useful for hybrid op/state systems where the context of an operation is
%% needed at a replica without sending the entire state to the client.
-spec precondition_context(orset()) -> binary_orset().
precondition_context({ADict, _RDict}) ->
    to_binary({ADict, orddict:new()}).

-define(TAG, 76).
-define(V1_VERS, 1).

-spec to_binary(orset()) -> binary_orset().
to_binary(ORSet) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(ORSet))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-ifdef(EQC).
eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, 1000).

%% EQC generator
gen_op() ->
    oneof([gen_updates(), gen_update()]).

gen_updates() ->
     {update, non_empty(list(gen_update()))}.

gen_update() ->
    oneof([{add, int()}, {remove, int()},
           {add_all, list(int())},
           {remove_all, list(int())}]).

init_state() ->
    {0, dict:new()}.

do_updates(_ID, [], _OldState, NewState) ->
    NewState;
do_updates(ID, [{_Action, []} | Rest], OldState, NewState) ->
    do_updates(ID, Rest, OldState, NewState);
do_updates(ID, [Update | Rest], OldState, NewState) ->
    case {Update, update_expected(ID, Update, NewState)} of
        {{Op, Arg}, NewState} when Op == remove;
                                   Op == remove_all ->
            %% precondition fail, or idempotent remove?
            {_Cnt, Dict} = NewState,
            {_A, R} = dict:fetch(ID, Dict),
            Removed = [ E || {E, _X} <- sets:to_list(R)],
            case member(Arg, Removed) of
                true ->
                    do_updates(ID, Rest, OldState, NewState);
                false ->
                    OldState
            end;
        {_, NewNewState} ->
            do_updates(ID, Rest, OldState, NewNewState)
    end.

member(_Arg, []) ->
    false;
member(Arg, L) when is_list(Arg) ->
    sets:is_subset(sets:from_list(Arg), sets:from_list(L));
member(Arg, L) ->
    lists:member(Arg, L).

update_expected(ID, {update, Updates}, State) ->
    do_updates(ID, lists:sort(Updates), State, State);
update_expected(ID, {add, Elem}, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Elem, Cnt},
    {A, R} = dict:fetch(ID, Dict),
    {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)};
update_expected(ID, {remove, Elem}, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    ToRem = [ {E, X} || {E, X} <- sets:to_list(A), E == Elem],
    {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)};
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)};
update_expected(ID, {add_all, Elems}, State) ->
    lists:foldl(fun(Elem, S) ->
                       update_expected(ID, {add, Elem}, S) end,
               State,
               Elems);
update_expected(ID, {remove_all, Elems}, {_Cnt, Dict}=State) ->
    %% Only if _all_ elements are in the set do we remove any elems
    {A, R} = dict:fetch(ID, Dict),
    Members = [E ||  {E, _X} <- sets:to_list(sets:union(A,R))],
    case sets:is_subset(sets:from_list(Elems), sets:from_list(Members)) of
        true ->
            lists:foldl(fun(Elem, S) ->
                                update_expected(ID, {remove, Elem}, S) end,
                        State,
                        Elems);
        false ->
            State
    end.


eqc_state_value({_Cnt, Dict}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Values = [ Elem || {Elem, _X} <- sets:to_list(Remaining)],
    lists:usort(Values).

-endif.

-endif.
