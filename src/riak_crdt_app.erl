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
            riak_core:register(riak_crdt, [
                {vnode_module, riak_crdt_vnode}
            ]),

            riak_crdt_wm_pncounter:add_routes(),
            riak_crdt_wm_orset:add_routes(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
