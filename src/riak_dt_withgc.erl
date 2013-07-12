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
-export([gc_propose/2, gc_execute/2]).
-export([new_epoch/1, epoch_actor/1]).

-ifdef(EQC).
% -include_lib("eqc/include/eqc.hrl").
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

update(How, Actor, #dt_withgc{mod=Mod, dt=Inner} = DT) ->
    DT#dt_withgc{dt=Mod:update(How, Actor, Inner)}.

% When the epochs are the same, merge inner values
% We shoudl really check whether one DT is just a sublog of the other.
% If it's not, we have to do some whacky shit instead.
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

gc_propose(Actor, #dt_withgc{mod=Mod, dt=Inner}) ->
    case Mod:gc_propose(Actor, Inner) of
        dont_gc_me_bro -> dont_gc_me_bro;
        GCOperation -> {?MODULE, new_epoch(Actor), GCOperation}
    end.

gc_execute(dont_gc_me_bro, DT) ->
    DT;
gc_execute({?MODULE, Epoch, Op}=GcOp,
           #dt_withgc{mod=Mod, dt=Inner0, gc_log=Log}=DT) ->
    Inner1 = Mod:gc_execute(Op, Inner0),
    Log1 = compact([GcOp | Log]),
    DT#dt_withgc{dt=Inner1, epoch=Epoch, gc_log=Log1}.

%% This is only an idea right now, but could be interesting.
% gc_undo(OldEpoch,
%         #dt_withgc{}=DT) ->
%     DT.

-ifdef(EQC).
%% ---
% [EQC Goes Here]
%% ---
-endif.


% So this function is used by merge to get a sibling to catch up with another
% before we can do a merge of the inner data types.
% Importantly we have to do the oldest GC operation first, so catchup log has the oldest one first.
catchup(#dt_withgc{epoch=Ep}=DT0, OtherLog) ->
    CatchupLog = prepare_catchup_log(Ep, OtherLog, []),
    lists:foldl(fun gc_execute/2, DT0, CatchupLog).

prepare_catchup_log(_Epoch, [], _CatchupLog) ->
    throw(missing_gc_operations);
prepare_catchup_log(Epoch, [{?MODULE, GcEpoch,_}=GcOp|Rest], CatchupLog) ->
    case epoch_compare(Epoch, GcEpoch) of
        lt -> prepare_catchup_log(Epoch, Rest, [GcOp|CatchupLog]);
        eq -> CatchupLog;
        gt -> throw(wrong_gc_order)
    end;
prepare_catchup_log(Epoch, [_Invalid | Rest], CatchupLog) ->
    prepare_catchup_log(Epoch, Rest, CatchupLog).

% Here we have the opportunity to get rid of old logs, or to compress them to
% save space.
compact(Log) ->
    Log.

new_epoch(Actor) ->
    {Actor, erlang:now()}.

epoch_actor({Actor, _TS}) ->
    Actor.

epoch_compare({_Actor1, TS1}=_Epoch1, {_Actor2, TS2}=_Epoch2) ->
    if
        TS1 < TS2 -> lt;
        TS1 == TS2 -> eq;
        TS1 > TS2 -> gt
    end.
