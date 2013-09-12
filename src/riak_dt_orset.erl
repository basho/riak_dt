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
-export([new/0, value/1, update/3, merge/2, equal/2, reset/2,
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

-export_type([orset/0, binary_orset/0, orset_op/0]).
-opaque orset() :: orddict:orddict(member(), uniques()).

-type binary_orset() :: binary(). %% A binary that from_binary/1 will operate on.

-type orset_op() :: {add, member()} | {remove, member()} |
                    {add_all, [member()]} | {remove_all, [member()]} |
                    {update, [orset_op()]}.

-type actor() :: riak_dt:actor().

%% an ordset
-type uniques() :: orddict:orddict(unique(),boolean()).
%% 0..2^27-1
%% @see phash/2
-type unique() :: non_neg_integer().
-type member() :: term().

-spec new() -> orset().
new() ->
    orddict:new().

-spec value(orset()) -> [member()].
value(ORDict0) ->
    ORDict1 = orddict:filter(fun(_Elem, Tokens) ->
            ValidTokens = [Token || {Token, false} <- orddict:to_list(Tokens)],
            length(ValidTokens) > 0
        end, ORDict0),
    orddict:fetch_keys(ORDict1).

-spec value(any(), orset()) -> [member()].
value(_,ORSet) ->
    value(ORSet).

-spec update(orset_op(), actor(), orset()) -> {ok, orset()} |
                                              {error, {precondition ,{not_present, member()}}}.
update({add,Elem}, Actor, ORDict) ->
    Token = unique(Actor),
    add_elem(Elem,Token,ORDict);
update({add_all,Elems}, Actor, ORDict0) ->
    OD = lists:foldl(fun(Elem,ORDict) ->
                {ok, ORDict1} = update({add,Elem},Actor,ORDict),
                ORDict1
            end, ORDict0, Elems),
    {ok, OD};
update({remove,Elem}, _Actor, ORDict) ->
    remove_elem(Elem, ORDict);
update({remove_all,Elems}, _Actor, ORDict0) ->
    remove_elems(Elems, ORDict0);
update({update, Ops}, Actor, ORDict) ->
    apply_ops(lists:sort(Ops), Actor, ORDict).

-spec merge(orset(), orset()) -> orset().
merge(ORDictA, ORDictB) ->
    orddict:merge(fun(_Elem,TokensA,TokensB) ->
            orddict:merge(fun(_Token,BoolA,BoolB) ->
                    BoolA or BoolB
                end, TokensA, TokensB)
        end, ORDictA, ORDictB).

-spec equal(orset(), orset()) -> boolean().
equal(ORDictA, ORDictB) ->
    ORDictA == ORDictB. % Everything inside is ordered, so this should work

%% @Doc reset the set to empty, eseentialy have `Actor' remove all
%% present members.  Equivalent to update({remove_all,
%% value(`ORSet')}, `Actor', `ORset')
-spec reset(orset(), actor()) -> orset().
reset(ORDict, Actor) ->
    Values = value(ORDict),
    {ok, ORDict1} = update({remove_all, Values}, Actor, ORDict),
    ORDict1.

%% @doc the precondition context is a fragment of the CRDT that
%% operations with pre-conditions can be applied too.  In the case of
%% OR-Sets this is the set of adds observed.  The system can then
%% apply a remove to this context and merge it with a replica.
%% Especially useful for hybrid op/state systems where the context of
%% an operation is needed at a replica without sending the entire
%% state to the client.
-spec precondition_context(orset()) -> orset().
precondition_context(ORDict) ->
    ordict:fold(fun(Elem, Tokens, ORDict1) ->
            case minimum_tokens(Tokens) of
                []      -> ORDict1;
                Tokens1 -> orddict:store(Elem, Tokens1, ORDict1)
            end
        end, orddict:new(), ORDict).

-define(TAG, 76).
-define(V1_VERS, 1).

-spec to_binary(orset()) -> binary_orset().
to_binary(ORSet) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(ORSet))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

%% Private
add_elem(Elem,Token,ORDict) ->
    case orddict:find(Elem, ORDict) of
        {ok, Tokens} -> Tokens1 = orddict:store(Token, false, Tokens),
                        {ok, orddict:store(Elem, Tokens1, ORDict)};
        error        -> Tokens = orddict:store(Token, false, orddict:new()),
                        {ok, orddict:store(Elem, Tokens, ORDict)}
    end.

remove_elem(Elem, ORDict) ->
    case orddict:find(Elem, ORDict) of
        {ok, Tokens} -> Tokens1 = orddict:fold(fun(Token, _, Tokens0) ->
                                orddict:store(Token, true, Tokens0)
                            end, orddict:new(), Tokens),
                        {ok, orddict:store(Elem, Tokens1, ORDict)};
        error        -> {error, {precondition, {not_present, Elem}}}
    end.


remove_elems([], ORDict) ->
    {ok, ORDict};
remove_elems([Elem|Rest], ORDict) ->
    case remove_elem(Elem,ORDict) of
        {ok, ORDict1} -> remove_elems(Rest, ORDict1);
        Error         -> Error
    end.


apply_ops([], _Actor, ORDict) ->
    {ok, ORDict};
apply_ops([Op | Rest], Actor, ORDict) ->
    case update(Op, Actor, ORDict) of
        {ok, ORDict1} -> apply_ops(Rest, Actor, ORDict1);
        Error -> Error
    end.

unique(Actor) ->
    erlang:phash2({Actor, erlang:now()}).

minimum_tokens(Tokens) ->
    orddict:filter(fun(_Token, Removed) ->
            not Removed
        end, Tokens).

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
