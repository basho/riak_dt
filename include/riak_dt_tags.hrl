%%% Tags for all our riak_dt CRDTs.
%%%
%%% Versions still live in the files themselves, allowing each data type to
%%% decide when it's a different version on its own.
%%%
%%% The simplest thing to do is this: when adding a new data type, insert a new
%%% line in this file with a unique tag number (trying to keep the file
%%% organised). Then in the riak_dt_<type>.erl file, have a line like so:
%%%
%%%   -include("riak_dt_tags.hrl").
%%%   -define(TAG, ?DT_<type>_TAG).
%%%
%%% Then use ?TAG in the to_/from_binary kerfuffle.

%% Flags: [73,74,79,80]
-define(DT_ENABLE_FLAG_TAG, 79).
-define(DT_DISABLE_FLAG_TAG, 80).
-define(DT_OD_FLAG_TAG, 73).
-define(DT_OE_FLAG_TAG, 74).

%% Registers: [72] + [90-99]
-define(DT_LWWREG_TAG, 72).
-define(DT_MAXREG_TAG, 90).
-define(DT_MINREG_TAG, 91).
-define(DT_RANGE_TAG, 92).

%% Counters: [70,71]
-define(DT_GCOUNTER_TAG, 70).
-define(DT_PNCOUNTER_TAG, 71).
-define(DT_EMCNTR_TAG, 85).

%% Sets: [75,76,82]
-define(DT_GSET_TAG, 82).
-define(DT_ORSET_TAG, 76).
-define(DT_ORSWOT_TAG, 75).

%% Maps: [77]
-define(DT_MAP_TAG, 77).

%% Other:
