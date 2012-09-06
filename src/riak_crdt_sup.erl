-module(riak_crdt_sup).

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
    VMaster = { riak_crdt_vnode_master,
                {riak_core_vnode_master, start_link, [riak_crdt_vnode]},
                permanent, 5000, worker, [riak_core_vnode_master]},

    UpdateFsmSup = {riak_crdt_update_fsm_sup,
                    {riak_crdt_update_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [riak_crdt_update_fsm_sup]},

    ValueFsmSup = {riak_crdt_value_fsm_sup,
                    {riak_crdt_value_fsm_sup, start_link, []},
                    permanent, infinity, supervisor, [riak_crdt_value_fsm_sup]},

    { ok,
      { {one_for_one, 5, 10},
        [VMaster, UpdateFsmSup, ValueFsmSup]}}.
