-module(riak_crdt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case riak_crdt_sup:start_link() of
        {ok, Pid} ->
            ok = lager:start(),
            ok = riak_core:register_vnode_module(riak_crdt_vnode),
            ok = riak_core_ring_events:add_guarded_handler(riak_crdt_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(riak_crdt_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(riak_crdt, self()),
            riak_crdt_wm_pncounter:add_routes(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
