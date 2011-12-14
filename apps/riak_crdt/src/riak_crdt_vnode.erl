-module(riak_crdt_vnode).
-behaviour(riak_core_vnode).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("riak_crdt.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

%% CRDT API
-export([value/4,
         update/4,
         merge/5,
         repair/4]).

-type key() :: {Mod :: atom(), Key :: term()}.

-record(state, {partition, node, storage_state}).

-define(MASTER, riak_crdt_vnode_master).
-define(sync(PrefList, Command, Master),
        riak_core_vnode_master:sync_command(PrefList, Command, Master)).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

value(PrefList, Mod, Key, ReqId) ->
    riak_core_vnode_master:command(PrefList, {value, Mod, Key, ReqId}, {fsm, undefined, self()}, ?MASTER).

%% Call sync, at source
update(IdxNode, Mod, Key, Args) ->
    ?sync(IdxNode, {update, Mod, Key, Args}, ?MASTER).

%% Call async at replica
merge(PrefList, Mod, Key, CRDT, ReqId) ->
    riak_core_vnode_master:command(PrefList, {merge, Mod, Key, CRDT, ReqId}, {fsm, undefined, self()}, ?MASTER).

%% Call aysnc at replica, just a merge with no reply
repair(PrefList, Mod, Key, CRDT) ->
    riak_core_vnode_master:command(PrefList, {merge, Mod, Key, CRDT, ignore}, ignore, ?MASTER).

%% Vnode API
init([Partition]) ->
    Node = node(),
    StorageState = start_storage(Partition),
    {ok, #state { partition=Partition, node=Node, storage_state=StorageState }}.

handle_command({value, Mod, Key, ReqId}, Sender, #state{partition=Idx, node=Node, storage_state=StorageState}=State) ->
    lager:debug("value ~p ~p~n", [Mod, Key]),
    Reply = lookup({Mod, Key}, StorageState),
    riak_core_vnode:reply(Sender, {ReqId, {{Idx, Node}, Reply}}),
    {noreply, State};
handle_command({update, Mod, Key, Args}, _Sender, #state{storage_state=StorageState, partition=Idx}=State) ->
    lager:debug("update ~p ~p ~p~n", [Mod, Key, Args]),
    Current = case lookup({Mod, Key}, StorageState) of
                  {Mod, Val} ->
                      Val;
                  notfound ->
                      %% Not found, so create locally
                      Mod:new()
              end,
    Updated = Mod:update(Args, {node(), Idx}, Current),
    ok = store({{Mod, Key}, {Mod, Updated}}, StorageState),
    Reply =   {ok, {Mod, Updated}},
    {reply, Reply, State};
handle_command({merge, Mod, Key, {Mod, RemoteVal}, ReqId}, Sender, #state{storage_state=StorageState}=State) ->
    lager:debug("Merge ~p ~p~n", [Mod, Key]),
    Reply = do_merge({Mod, Key}, RemoteVal, StorageState),
    riak_core_vnode:reply(Sender, {ReqId, Reply}),
    {noreply, State};
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State=#state{storage_state=StorageState}) ->
    Acc = fold(Fun, Acc0, StorageState),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Binary, #state{storage_state=StorageState}=State) ->
    {{Mod, Key}, {Mod, HoffVal}} = binary_to_term(Binary),
    %% merge with local
    ok = do_merge({Mod, Key}, HoffVal, StorageState),
    {reply, ok, State}.

encode_handoff_item(Name, Value) ->
    term_to_binary({Name, Value}).

is_empty(State) ->
    {db_empty(State#state.storage_state), State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% priv
do_merge({Mod, Key}, RemoteVal, StorageState) ->
    Merged = case lookup({Mod, Key}, StorageState) of
                 {Mod, LocalVal} ->
                     Mod:merge(LocalVal, RemoteVal);
                 notfound ->
                     RemoteVal
             end,
    store({{Mod, Key}, {Mod, Merged}}, StorageState).

%% Priv, storage stuff
-spec start_storage(Partition :: integer()) ->
                           StorageState :: term().
start_storage(Partition) ->
    Table = "data/" ++ integer_to_list(Partition),
    {ok, Table} = dets:open_file(Table, []),
    Table.

-spec lookup(K :: key(), StorageState :: term()) ->
                   {atom(), term()} | notfound.
lookup(Key, StorageState) ->
    Reply = case dets:lookup(StorageState, Key) of
                [{{Mod, Key}, {Mod, Val}=Val}] ->
                    Val;
                [] ->
                    notfound
            end,
    Reply.

-spec store({K :: key(), {atom(), term()}}, StorageState :: term()) ->
                   ok.
store(KV, StorageState) ->
    dets:insert(StorageState, KV).

-spec fold(function(), list(), StorageState :: term()) ->
                  list().
fold(Fun, Acc0, StorageState) ->
    F = fun({K, V}, AccIn) -> Fun(K, V, AccIn) end,
    dets:foldl(F, Acc0, StorageState).

-spec is_empty(StorageState :: term()) ->
                      boolean().
db_empty(StorageState) ->
    case dets:info(StorageState, no_keys) of
        0 -> true;
        _ -> false
    end.
