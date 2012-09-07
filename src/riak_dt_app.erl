-module(riak_dt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case riak_dt_sup:start_link() of
        {ok, Pid} ->
            ok = lager:start(),
            riak_core:register(riak_dt, [
                {vnode_module, riak_dt_vnode}
            ]),

            riak_dt_wm_pncounter:add_routes(),
            riak_dt_wm_orset:add_routes(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
