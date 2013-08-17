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

-module(riak_dt_gc).

-export([meta/4, meta/3]).
-export([new_epoch/1, epoch_actor/1, epoch_compare/2]).

-include("riak_dt_gc_meta.hrl").

-type epoch() :: {riak_dt:actor(), erlang:timestamp()}.
-type crdt_fragment() :: term().
-export_type([gc_meta/0, epoch/0, crdt_fragment/0]).

-callback gc_epoch(riak_dt:crdt()) -> epoch().
-callback gc_ready(gc_meta(), riak_dt:crdt()) -> boolean().
-callback gc_get_fragment(gc_meta(), riak_dt:crdt()) -> crdt_fragment().
-callback gc_replace_fragment(gc_meta(), crdt_fragment(), riak_dt:crdt()) -> riak_dt:crdt().

-spec meta(riak_dt:actor(), [riak_dt:actor()], float()) -> gc_meta().
meta(Epoch, PActors, CompactThreshold) ->
    meta(Epoch, PActors, [], CompactThreshold).

-spec meta(riak_dt:actor(), [riak_dt:actor()], [riak_dt:actor()], float()) -> gc_meta().
meta(Epoch, PActors, ROActors, CompactThreshold) ->
    ?GC_META{epoch=Epoch,
             actor=epoch_actor(Epoch),
             primary_actors=ordsets:from_list(PActors),
             readonly_actors=ordsets:from_list(ROActors),
             compact_threshold=CompactThreshold}.

-spec new_epoch(riak_dt:actor()) -> epoch().
new_epoch(Actor) ->
    {Actor, erlang:now()}.

-spec epoch_actor(epoch()) -> riak_dt:actor().
epoch_actor({Actor, _TS}) ->
    Actor.

-spec epoch_compare(epoch(), epoch()) -> lt | eq | gt.
epoch_compare({_Actor1, TS1}=_Epoch1, {_Actor2, TS2}=_Epoch2) ->
    if
        TS1 < TS2 -> lt;
        TS1 == TS2 -> eq;
        TS1 > TS2 -> gt
    end.
