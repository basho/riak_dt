-module(riak_dt_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = { riak_dt_vnode_master,
                {riak_core_vnode_master, start_link, [riak_dt_vnode]},
                permanent, 5000, worker, [riak_core_vnode_master]},

    UpdateFsmSup = {riak_dt_update_fsm_sup,
                    {riak_dt_update_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [riak_dt_update_fsm_sup]},

    ValueFsmSup = {riak_dt_value_fsm_sup,
                    {riak_dt_value_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [riak_dt_value_fsm_sup]},

    { ok,
      { {one_for_one, 5, 10},
        [VMaster, UpdateFsmSup, ValueFsmSup]}}.
