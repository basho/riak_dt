
-type actor() :: term().
-type epoch() :: {actor(), erlang:timestamp()}.

-define(GC_META, #riak_dt_gc_meta).
-define(GC_META_ACTOR(GM), GM#riak_dt_gc_meta.actor).
-record(riak_dt_gc_meta,
        {
          actor :: actor(),             % Actor performing the GC
          primary_actors :: [actor()],  % Actors most likely to be involved in operations
          readonly_actors :: [actor()], % Actors that can't be GCd (ie cluster remotes)
          compact_proportion :: float()  % Max Proportion of non-primary actors or tombstones
        }).
-opaque gc_meta() :: #riak_dt_gc_meta{}.