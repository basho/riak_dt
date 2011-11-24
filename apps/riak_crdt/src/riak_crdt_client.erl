%% @doc Interface into the CRDT application.
-module(riak_crdt_client).
-include("riak_crdt.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
        update/3,
        value/1
        ]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Tell the crdt at key to update itself with Args.
update(Key, Mod, Args) ->
    ReqID = mk_reqid(),
    riak_crdt_update_fsm_sup:start_update_fsm(node(), [ReqID, self(), Key, Mod, Args]),
    wait_for_reqid(ReqID, 5000).
    
value(Key) ->
    riak_crdt_vnode:value(get_idxnode(Key), Key).

%%%===================================================================
%%% Internal Functions
%%%===================================================================
get_idxnode(Key) ->
    DocIdx = riak_core_util:chash_key({list_to_binary(Key), list_to_binary(Key)}),
    hd(riak_core_apl:get_apl(DocIdx, 1, riak_crdt)).


mk_reqid() -> erlang:phash2(erlang:now()).

wait_for_reqid(ReqID, Timeout) ->
    receive
	{ReqID, ok} -> ok;
        {ReqID, ok, Val} -> {ok, Val};
        {ReqId, Error} -> Error
    after Timeout ->
	    {error, timeout}
    end.
