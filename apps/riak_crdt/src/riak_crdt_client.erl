%% @doc Interface into the CRDT application.
-module(riak_crdt_client).
-include("riak_crdt.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
        update/3,
        value/2
        ]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Tell the crdt at key to update itself with Args.
update(Mod, Key, Args) ->
    ReqID = mk_reqid(),
    riak_crdt_update_fsm_sup:start_update_fsm(node(), [ReqID, self(), Mod, Key, Args]),
    wait_for_reqid(ReqID, 5000).
    
value(Mod, Key) ->
    ReqID = mk_reqid(),
    riak_crdt_value_fsm_sup:start_value_fsm(node(), [ReqID, self(), Mod, Key]),
    wait_for_reqid(ReqID, 5000).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
mk_reqid() -> erlang:phash2(erlang:now()).

wait_for_reqid(ReqID, Timeout) ->
    receive
	{ReqID, ok} -> ok;
        {ReqID, ok, Val} -> {ok, Val};
        {ReqID, Error} -> Error
    after Timeout ->
	    {error, timeout}
    end.
