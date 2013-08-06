
-type actor() :: term().
-type epoch() :: {actor(), erlang:timestamp()}.

-define(GC_META, #riak_dt_gc_meta).
-define(GC_META_ACTOR(GM), GM?GC_META.actor).
-record(riak_dt_gc_meta,
        {
          epoch :: epoch(),             % Epoch of GC
          actor :: actor(),             % Actor performing the GC
          primary_actors :: [actor()],  % Actors most likely to be involved in operations
          readonly_actors :: [actor()], % Actors that can't be GCd (ie cluster remotes)
          compact_threshold :: float()  % Float between 0.0 and 1.0
        }).
-opaque gc_meta() :: #riak_dt_gc_meta{}.

% A Helper, put in Calced as from the notes below. Doesn't cope with
% 'badarith' errors, so wrap in a case statement.
-define(SHOULD_GC(Meta,CalcedVal), CalcedVal < Meta?GC_META.compact_threshold).

%% the compact_threshold is a number between 0.0 and 1.0:
%%
%% - closer to 0.0 means compact less often, but compact more bytes 
%%   (ie few large compactions)
%% - closer to 1.0 means compact more often, but compact less bytes
%%   (ie many small compactions)
%%
%% We compact if a calculated value is between 0.0 and the compact_threshold
%% (i.e. we compact as much as possible each time)
%%
%% For example:
%% GCounters: Compacted when ROActors/TotalActors   < compact_threshold
%% ORSets:    Compacted when (1 - Tombstones/Total) < compact_threshold
%% 
%% To make this simple, choose one of two possible calculations:
%% * Calced = KeptSize / CurrentSize
%% * Calced = 1 - (RemovedSize / CurrentSize)
