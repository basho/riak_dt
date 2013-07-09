%% -------------------------------------------------------------------
%%
%% riak_dt_withgc.erl: behaviour for convergent data types that support
%% consensus-based GC
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

-module(riak_dt_withgc).

-export([new/1, value/1, value/2, update/3, merge/2, equal/2]).

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
% -export([gen_op/0, update_expected/3, eqc_state_value/1]).
-endif.

-record(dt_withgc, {
    mod :: module(),    % The "type" of the inner CRDT
    dt :: term(),       % The inner CRDT
    gc_log = [] :: [{term(), term()}],
                        % Operations carried out in previous GCs
    epoch = undefined :: term()     % The id of the most recent GC
    }).

new(Mod) ->
    #dt_withgc{mod=Mod, dt=Mod:new()}.

value(#dt_withgc{mod=Mod, dt=Inner}) ->
    Mod:value(Inner).

value(What, #dt_withgc{mod=Mod, dt=Inner}) ->
    Mod:value(What, Inner).

update(How, #dt_withgc{mod=Mod, dt=Inner} = DT) ->
    DT#dt_withgc{dt=Mod:update(How, Inner)}.

% When the epochs are the same, merge inner values
merge(#dt_withgc{mod=Mod, dt=Inner1, epoch=Ep1}=DT1,
      #dt_withgc{mod=Mod, dt=Inner2, epoch=Ep2}=DT2) ->
    case epoch_compare(Ep1, Ep2) of
        % Epochs equal: just merge
        eq -> DT1#dt_withgc{dt=Mod:merge(Inner1,Inner2)};
        % DT1 needs to catch up with DT2 before merge
        lt -> DT1a = catchup(DT1, DT2#dt_withgc.gc_log),
              merge(DT1a,DT2);
        % DT2 needs to catch up with DT1 before merge
        gt -> DT2a = catchup(DT2, DT1#dt_withgc.gc_log),
              merge(DT2a,DT1)
    end.

% Check epochs in the equality comparison
equal(#dt_withgc{mod=Mod, dt=Inner1, epoch=Ep1},
      #dt_withgc{mod=Mod, dt=Inner2, epoch=Ep2}) ->
    epoch_compare(Ep1,Ep2) == eq andalso Mod:equal(Inner1, Inner2).

-spec gc_propose(term(), term()) -> {term(), tuple()}
gc_propose(Actor, #dt_withgc{mod=Mod, dt=Inner}) ->
    {new_epoch(Actor), Mod:gc_propose(Actor, Inner)}.

-spec gc_execute({term(), tuple()}, term()) -> term().
gc_execute({Epoch, Op}=GcOp,
           #dt_withgc{mod=Mod, dt=Inner0, epoch=Ep, gc_log=Log0}=DT) ->
    Inner1 = Mod:gc_execute(Op, Inner0),
    Log1 = compact([GcOp | Log]),
    DT#dt_withgc{dt=Inner1, epoch=Epoch, gc_log=Log1}.

%% This is only an idea right now, but could be interesting.
% gc_undo(OldEpoch,
%         #dt_withgc{}=DT) ->
%     DT.

%% ---
% [EQC Goes Here]
%% ---


% So this function is used by merge to get a sibling to catch up with another
% before we can do a merge of the inner data types.
% First we get a log of operations to perform, 
catchup(#dt_withgc{mod=Mod, dt=Inner0, epoch=Ep, gc_log=Log}=DT, OtherLog0) ->
    % TODO: catchup stuff goes here
    DT.

% Here we have the opportunity to get rid of old logs, or to compress them to 
% save space.
compact(Log) ->
    Log.

new_epoch(Actor) ->
    {Actor, erlang:now()}.
    
epoch_actor({Actor, _TS}) ->
    Actor.

epoch_compare({_Actor1, TS1}=Epoch1, {_Actor2, TS2}=Epoch2) ->
    if
        TS1 < TS2 -> lt;
        TS1 == TS2 -> eq;
        TS1 > TS2 -> gt;
    end.
