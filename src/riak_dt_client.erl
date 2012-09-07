%% @doc Interface into the CRDT application.
-module(riak_dt_client).
-include("riak_dt.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         update/3,
         update/4,
         value/2,
         value/3
        ]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Tell the crdt at key to update itself with Args.
update(Mod, Key, Args) ->
    update(Mod, Key, Args, 5000).
update(Mod, Key, Args, Timeout) ->
    ReqID = mk_reqid(),
    {ok, _} = riak_dt_update_fsm_sup:start_update_fsm(node(), [ReqID, self(), Mod, Key, Args]),
    wait_for_reqid(ReqID, Timeout).
value(Mod, Key) ->
    value(Mod, Key, 5000).
value(Mod, Key, Timeout) ->
    ReqID = mk_reqid(),
    {ok, _} = riak_dt_value_fsm_sup:start_value_fsm(node(), [ReqID, self(), Mod, Key]),
    wait_for_reqid(ReqID, Timeout).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
mk_reqid() -> erlang:phash2(erlang:now()).

wait_for_reqid(ReqID, Timeout) ->
    receive
        {ReqID, ok} -> ok;
        {ReqID, ok, Val} -> {ok, Val};
        {ReqID, Error} -> Error;
        Other ->
            lager:error("Received unexpected message! ~w~n", [Other]),
            Other
    after Timeout ->
	    {error, timeout}
    end.
