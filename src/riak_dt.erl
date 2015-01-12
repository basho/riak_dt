%% -------------------------------------------------------------------
%%
%% riak_dt.erl: behaviour for convergent data types
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

-module(riak_dt).

-export([get_deferred/1, get_deferred/2, to_binary/1, from_binary/1, dict_to_orddict/1]).
-export_type([actor/0, dot/0, crdt/0, context/0]).

-type crdt() :: term().
-type operation() :: term().
-type actor() :: term().
-type value() :: term().
-type error() :: term().
-type dot() :: {actor(), pos_integer()}.
-type context() :: riak_dt_vclock:vclock() | undefined.

-callback new() -> crdt().
-callback value(crdt()) -> term().
-callback value(term(), crdt()) -> value().
-callback update(operation(), actor(), crdt()) -> {ok, crdt()} | {error, error()}.
-callback update(operation(), actor(), crdt(), context()) -> {ok, crdt()} | {error, error()}.
%% @doc When nested in a Map, some CRDTs need the logical clock of the
%% top level Map to make context operations. This callback provides
%% the clock and the crdt, and if relevant, returns to crdt with the
%% given clock as it's own.
-callback parent_clock(riak_dt_vclock:vclock(), crdt()) ->
     crdt().
-callback get_deferred(crdt()) -> [riak_dt_vclock:vclock()].
-callback get_deferred(crdt(), riak_dt_vclock:vclock()) -> [riak_dt_vclock:vclock()].
-callback merge(crdt(), crdt()) -> crdt().
-callback equal(crdt(), crdt()) -> boolean().
-callback to_binary(crdt()) -> binary().
-callback to_binary(TargetVers :: pos_integer(), crdt()) ->
     binary().
-callback from_binary(binary()) -> crdt().
-callback from_binary(TargetVers :: pos_integer(), binary()) ->
    crdt().

-callback stats(crdt()) -> [{atom(), number()}].
-callback stat(atom(), crdt()) -> number() | undefined.

-ifdef(EQC).
% Extra callbacks for any crdt_statem_eqc tests

-callback gen_op() -> eqc_gen:gen(operation()).

-endif.

-spec to_binary(crdt()) -> binary().
to_binary(Term) ->
    Opts = case application:get_env(riak_dt, binary_compression, 1) of
               true -> [compressed];
               N when N >= 0, N =< 9 -> [{compressed, N}];
               _ -> []
           end,
    term_to_binary(Term, Opts).

-spec from_binary(binary()) -> crdt().
from_binary(Binary) ->
    binary_to_term(Binary).


%% @private
-spec dict_to_orddict(dict()) -> orddict:orddict().
dict_to_orddict(Dict) ->
    orddict:from_list(dict:to_list(Dict)).

-spec get_deferred(crdt()) -> [riak_dt_vclock:vclock()].
get_deferred({_, _, Deferred}) -> Deferred.

-spec get_deferred(crdt(), riak_dt_vclock:vclock()) -> [riak_dt_vclock:vclock()].
get_deferred({_, _, Deferred}, Ctx) ->
    lists:filtermap(fun(Def) ->
                            case riak_dt_vclock:glb(Def,Ctx) of
                                [] -> false;
                                Intersect -> {true, Intersect}
                            end
                    end, Deferred).

