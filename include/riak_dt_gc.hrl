
-type actor() :: term().

-define(GC_THRESHOLD, #riak_dt_gc_threshold).
-record(riak_dt_gc_threshold,
        {
          primary_actors :: [actor()], % Actors usually involved in 
          max_unneeded :: float()      % Max Proportion of non-primary actors or tombstones
        }).