%% ------------------------------------------------------------------- 
%%
%% riak_dt_tsmap: Tombstoning Map
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

%% @doc A Struct CRDT for holding other CRDTs
%%
%%
%% @end

-module(riak_dt_tsmapbeast).

-behaviour(riak_dt).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/0, value/1, value/2, update/3, merge/2,
         equal/2, to_binary/1, from_binary/1, precondition_context/1, stats/1, stat/2]).

%% EQC API
-ifdef(EQC).
-export([gen_op/0, update_expected/3, eqc_state_value/1,
         init_state/0, gen_field/0, generate/0, size/1]).
-endif.

-type map() :: {riak_dt_vclock:vclock(), entries()}.
-type entries() :: ordsets:ordsets(entry()).
-type entry() :: {Field :: field(), CRDT :: term(), Tag :: term(), Present :: boolean()}.
-type field() :: {binary(), module()}.
%% @doc Create a new, empty Map.
-spec new() -> map().
new() ->
    {riak_dt_vclock:fresh(), ordsets:new()}.

%% @doc get the current set of values for this Map
value({_Clock, Values}) ->
    Remaining = [{Field, CRDT} || {Field, CRDT, _Tag, InMap} <- ordsets:to_list(Values), InMap == true],
    Res = lists:foldl(fun({{_Name, Type}=Key, Value}, Acc) ->
                              %% if key is in Acc merge with it and replace
                              dict:update(Key, fun(V) ->
                                                       Type:merge(V, Value) end,
                                          Value, Acc) end,
                      dict:new(),
                      Remaining),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res)].

value(_, Map) ->
    value(Map).

update({update, Ops}, Actor, {Clock, Values}) ->
    apply_ops(Ops, Actor, Clock, Values).

%% @private
apply_ops([], _Actor, Clock, Values) ->
    {ok, {Clock, Values}};
apply_ops([{update, {Name, Type}=Field, Op} | Rest], Actor, Clock, Values) ->
    InMap = [{F, CRDT} || {F, CRDT, _Tag, InMap} <- ordsets:to_list(Values), InMap == true],
    CRDT = lists:foldl(fun({{FName, FType}, Value}, Acc) when FName == Name,
                                                                  FType == Type ->
                               Type:merge(Acc, Value);
                          (_, Acc) -> Acc
                       end,
                       Type:new(),
                       InMap),
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Tag = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},
    case Type:update(Op, Tag, CRDT) of
        {ok, Updated} ->
            NewValues = ordsets:add_element({Field, Updated, Tag, true}, Values),
            apply_ops(Rest, Actor, NewClock, NewValues);
        Error ->
            Error
    end;
apply_ops([{remove, Field} | Rest], Actor, Clock, Values) ->
    {Removed, NewValues} = ordsets:fold(fun({F, Val, Token, true}, {_B, AccIn}) when F == Field ->
                                     {true, ordsets:add_element({F, Val, Token, false}, AccIn)};
                                (Elem, {B, AccIn}) ->
                                     {B, ordsets:add_element(Elem, AccIn)}
                             end,
                             {false, ordsets:new()},
                             Values),
    case Removed of
        false ->
            {error, {precondition, {not_present, Field}}};
        _ ->
            apply_ops(Rest, Actor, Clock, NewValues)
    end;
apply_ops([{add, {_Name, Mod}=Field} | Rest], Actor, Clock, Values) ->
    NewClock = riak_dt_vclock:increment(Actor, Clock),
    Tag = {Actor, riak_dt_vclock:get_counter(Actor, NewClock)},

    %% InMap = [{F, CRDT} || {F, CRDT, _Tag, InMap} <- ordsets:to_list(Values), InMap == true],
    %% CRDT = lists:foldl(fun({{FName, FType}, Value}, Acc) when FName == Name,
    %%                                                               FType == Mod ->
    %%                            Mod:merge(Acc, Value);
    %%                       (_, Acc) -> Acc
    %%                    end,
    %%                    Mod:new(),
    %%                    InMap),
    ToAdd = {Field, Mod:new(), Tag, true},
    NewValues = ordsets:add_element(ToAdd, Values),
    apply_ops(Rest, Actor, NewClock, NewValues).


merge({LHSClock, LHSEntries}, {RHSClock, RHSEntries}) ->
    Clock = riak_dt_vclock:merge([LHSClock, RHSClock]),

    RHS0 = ordsets:to_list(RHSEntries),

    {Entries0, RHSUnique} = lists:foldl(fun({F, CRDT, Tag, _Bool}=E, {Acc, RHS}) ->
                        case lists:keytake(Tag, 3, RHS) of
                            {value, E, RHS1} ->
                                %% same in bolth
                                {ordsets:add_element(E, Acc), RHS1};
                            {value, {F, CRDT, Tag, true}, RHS1} ->
                                {ordsets:add_element(E, Acc), RHS1};
                            {value, {F, CRDT, Tag, false}=E1, RHS1} ->
                                {ordsets:add_element(E1, Acc), RHS1};
                            false ->
                                {ordsets:add_element(E, Acc), RHS}
                        end
                end,
                {ordsets:new(), RHS0},
                LHSEntries),

    Entries = ordsets:union(Entries0, ordsets:from_list(RHSUnique)),
    {Clock, Entries}.

equal({Clock1, Values1}, {Clock2, Values2}) ->
    riak_dt_vclock:equal(Clock1, Clock2) andalso
        pairwise_equals(ordsets:to_list(Values1), ordsets:to_list(Values2)).

pairwise_equals([], []) ->
    true;
pairwise_equals([{{_Name, Type}, CRDT1, Tag, Bool}| Rest1], [{{_Name, Type}, CRDT2, Tag, Bool}|Rest2]) ->
    case Type:equal(CRDT1, CRDT2) of
        true ->
            pairwise_equals(Rest1, Rest2);
        false ->
            false
    end;
pairwise_equals(_, _) ->
    false.

%% @Doc a "fragment" of the Map that can be used for precondition
%% operations. The schema is just the active Key Set The values are
%% just those values that are present We use either the values
%% precondition_context or the whole CRDT
-spec precondition_context(map()) -> map().
precondition_context(Map) ->
    Map.

stats(_) ->
    [].

stat(_,_) -> undefined.

-define(TAG, 101).
-define(V1_VERS, 1).

%% @doc returns a binary representation of the provided `map()'. The
%% resulting binary is tagged and versioned for ease of future
%% upgrade. Calling `from_binary/1' with the result of this function
%% will return the original map.  Use the application env var
%% `binary_compression' to turn t2b compression on (`true') and off
%% (`false')
%%
%% @see `from_binary/1'
to_binary(Map) ->
    Opts = case application:get_env(riak_dt, binary_compression, 1) of
               true -> [compressed];
               N when N >= 0, N =< 9 -> [{compressed, N}];
               _ -> []
           end,
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Map, Opts))/binary>>.

%% @doc When the argument is a `binary_map()' produced by
%% `to_binary/1' will return the original `map()'.
%%
%% @see `to_binary/1'
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, B/binary>>) ->
    binary_to_term(B).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% This fails on riak_dt_map
assoc_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    {ok, B} = update({update, [{update, Field, {add, 0}}]}, b, new()),
    {ok, B2} = update({update, [{update, Field, {remove, 0}}]}, b, B),
    C = A,
    {ok, C2} = update({update, [{add, Field}]}, c, C),
    {ok, C3} = update({update, [{remove, Field}]}, c, C2),
    ?assertEqual(merge(A, merge(B2, C3)), merge(merge(A, B2), C3)),
    ?assertEqual(value(merge(merge(A, C3), B2)), value(merge(merge(A, B2), C3))),
    ?assertEqual(merge(merge(A, C3), B2),  merge(merge(A, B2), C3)).

clock_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    B = A,
    {ok, B2} = update({update, [{update, Field, {add, 1}}]}, b, B),
    {ok, A2} = update({update, [{update, Field, {remove, 0}}]}, a, A),
    {ok, A3} = update({update, [{remove, Field}]}, a, A2),
    {ok, A4} = update({update, [{update, Field, {add, 2}}]}, a, A3),
    AB = merge(A4, B2),
    ?assertEqual([{Field, [1, 2]}], value(AB)).

remfield_test() ->
    Field = {'X', riak_dt_orswot},
    {ok, A} = update({update, [{update, Field, {add, 0}}]}, a, new()),
    B = A,
    {ok, A2} = update({update, [{update, Field, {remove, 0}}]}, a, A),
    {ok, A3} = update({update, [{remove, Field}]}, a, A2),
    {ok, A4} = update({update, [{update, Field, {add, 2}}]}, a, A3),
    AB = merge(A4, B),
    ?assertEqual([{Field, [2]}], value(AB)).

-ifdef(EQC).
-define(NUMTESTS, 1000).

bin_roundtrip_test_() ->
    crdt_statem_eqc:run_binary_rt(?MODULE, ?NUMTESTS).

eqc_value_test_() ->
    crdt_statem_eqc:run(?MODULE, ?NUMTESTS).

%% ===================================
%% crdt_statem_eqc callbacks
%% ===================================
size(Map) ->
    byte_size(term_to_binary(Map)) div 10.

generate() ->
        ?LET({Ops, Actors}, {non_empty(list(gen_op())), non_empty(list(bitstring(16*8)))},
         lists:foldl(fun(Op, Map) ->
                             Actor = case length(Actors) of
                                         1 -> hd(Actors);
                                         _ -> lists:nth(crypto:rand_uniform(1, length(Actors)), Actors)
                                     end,
                             case riak_dt_tsmap:update(Op, Actor, Map) of
                                 {ok, M} -> M;
                                 _ -> Map
                             end
                     end,
                     riak_dt_map:new(),
                     Ops)).


gen_op() ->
    ?LET(Ops, non_empty(list(gen_update())), {update, Ops}).

gen_update() ->
    ?LET(Field, gen_field(),
         oneof([{add, Field}, {remove, Field},
                {update, Field, gen_field_op(Field)}])).

gen_field() ->
    {non_empty(binary()), oneof([riak_dt_pncounter,
                                 riak_dt_orswot,
                                 riak_dt_lwwreg,
                                 riak_dt_tsmap])}.

gen_field_op({_Name, Type}) ->
    Type:gen_op().

init_state() ->
    {0, dict:new()}.

update_expected(ID, {update, Ops}, State) ->
    %% Ops are atomic, all pass or all fail
    %% return original state if any op failed
    update_all(ID, Ops, State);
update_expected(ID, {merge, SourceID}, {Cnt, Dict}) ->
    {FA, FR} = dict:fetch(ID, Dict),
    {TA, TR} = dict:fetch(SourceID, Dict),
    MA = sets:union(FA, TA),
    MR = sets:union(FR, TR),
    {Cnt, dict:store(ID, {MA, MR}, Dict)};
update_expected(ID, create, {Cnt, Dict}) ->
    {Cnt, dict:store(ID, {sets:new(), sets:new()}, Dict)}.

eqc_state_value({_Cnt, Dict}) ->
    {A, R} = dict:fold(fun(_K, {Add, Rem}, {AAcc, RAcc}) ->
                               {sets:union(Add, AAcc), sets:union(Rem, RAcc)} end,
                       {sets:new(), sets:new()},
                       Dict),
    Remaining = sets:subtract(A, R),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    [{K, Type:value(V)} || {{_Name, Type}=K, V} <- dict:to_list(Res)].

%% @private
%% @doc Apply the list of update operations to the model
update_all(ID, Ops, OriginalState) ->
    update_all(ID, Ops, OriginalState, OriginalState).

update_all(_ID, [], _OriginalState, NewState) ->
    NewState;
update_all(ID, [{update, {_Name, Type}=Key, Op} | Rest], OriginalState, {Cnt0, Dict}) ->
    CurrentValue = get_for_key(Key, ID, Dict),
    %% handle precondition errors any precondition error means the
    %% state is not changed at all
    case Type:update(Op, ID, CurrentValue) of
        {ok, Updated} ->
            Cnt = Cnt0+1,
            ToAdd = {Key, Updated, Cnt},
            {A, R} = dict:fetch(ID, Dict),
            update_all(ID, Rest, OriginalState, {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)});
        _Error ->
            OriginalState
    end;
update_all(ID, [{remove, Field} | Rest], OriginalState, {Cnt, Dict}) ->
    {A, R} = dict:fetch(ID, Dict),
    In = sets:subtract(A, R),
    PresentFields = [E ||  {E, _, _X} <- sets:to_list(In)],
    case lists:member(Field, PresentFields) of
        true ->
            ToRem = [{E, V, X} || {E, V, X} <- sets:to_list(A), E == Field],
            NewState2 = {Cnt, dict:store(ID, {A, sets:union(R, sets:from_list(ToRem))}, Dict)},
            update_all(ID, Rest, OriginalState, NewState2);
        false ->
            OriginalState
    end;
update_all(ID, [{add, {_Name, Type}=Field} | Rest], OriginalState, {Cnt0, Dict}) ->
    Cnt = Cnt0+1,
    ToAdd = {Field, Type:new(), Cnt},
    {A, R} = dict:fetch(ID, Dict),
    NewState = {Cnt, dict:store(ID, {sets:add_element(ToAdd, A), R}, Dict)},
    update_all(ID, Rest, OriginalState, NewState).


get_for_key({_N, T}=K, ID, Dict) ->
    {A, R} = dict:fetch(ID, Dict),
    Remaining = sets:subtract(A, R),
    Res = lists:foldl(fun({{_Name, Type}=Key, Value, _X}, Acc) ->
                        %% if key is in Acc merge with it and replace
                        dict:update(Key, fun(V) ->
                                                 Type:merge(V, Value) end,
                                    Value, Acc) end,
                dict:new(),
                sets:to_list(Remaining)),
    proplists:get_value(K, dict:to_list(Res), T:new()).

-endif.

query_test() ->
    Map = new(),
    {ok, Map1} = update({update, [{add, {c, riak_dt_pncounter}},
                                  {add, {s, riak_dt_orswot}},
                                  {add, {m, riak_dt_map}},
                                  {add, {l, riak_dt_lwwreg}},
                                  {add, {l2, riak_dt_lwwreg}}]}, a1, Map),
    ?assertEqual(5, value(size, Map1)),

    {ok, Map2} = update({update, [{remove, {l2, riak_dt_lwwreg}}]}, a2, Map1),
    ?assertEqual(4, value(size, Map2)),

    ?assertEqual([{c, riak_dt_pncounter},
                  {l, riak_dt_lwwreg},
                  {m, riak_dt_map},
                  {s, riak_dt_orswot}], value(keyset, Map2)),

    ?assert(value({contains, {c, riak_dt_pncounter}}, Map2)),
    ?assertNot(value({contains, {l2, riak_dt_lwwreg}}, Map2)),

    {ok, Map3} = update({update, [{update, {c, riak_dt_pncounter}, {increment, 33}},
                            {update, {l, riak_dt_lwwreg}, {assign, lww_val, 77}}]}, a3, Map2),

    ?assertEqual(33, value({get, {c, riak_dt_pncounter}}, Map3)),
    ?assertEqual({lww_val, 77}, value({get_crdt, {l, riak_dt_lwwreg}}, Map3)).


stat_test() ->
    Map = new(),
    {ok, Map1} = update({update, [{add, {c, riak_dt_pncounter}},
                                  {add, {s, riak_dt_orswot}},
                                  {add, {m, riak_dt_map}},
                                  {add, {l, riak_dt_lwwreg}},
                                  {add, {l2, riak_dt_lwwreg}}]}, a1, Map),
    {ok, Map2} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"foo">>, 1}}]}, a2, Map1),
    {ok, Map3} = update({update, [{update, {l, riak_dt_lwwreg}, {assign, <<"bar">>, 2}}]}, a3, Map1),
    Map4 = merge(Map2, Map3),
    ?assertEqual([{actor_count, 0}, {field_count, 0}, {max_dot_length, 0}], stats(Map)),
    ?assertEqual(3, stat(actor_count, Map4)),
    ?assertEqual(5, stat(field_count, Map4)),
    ?assertEqual(2, stat(max_dot_length, Map4)),
    ?assertEqual(undefined, stat(waste_pct, Map4)).

-endif.
