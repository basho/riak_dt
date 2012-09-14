%% -------------------------------------------------------------------
%%
%% riak_dt_vnode: Vnode for riak_dt storage / serialized access
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_dt_vnode).
-behaviour(riak_core_vnode).
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("riak_dt.hrl").

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

%% DT API
-export([value/4,
         update/4,
         merge/5,
         repair/4]).

-type key() :: {Mod :: atom(), Key :: term()}.

-type command() :: {value, Mod::module(), Key::term(), ReqId::term()} |
                   {update, Mod::module(), Key::term(), Args::list()} |
                   {merge, Mod::module(), Key::term(), {Mod::module(), RemoteVal::term()}, ReqId::term()}.

-record(state, {partition, node, storage_state, vnode_id, data_dir, storage_opts}).

-define(MASTER, riak_dt_vnode_master).
-define(sync(PrefList, Command, Master),
        riak_core_vnode_master:sync_command(PrefList, Command, Master)).

%% @doc Starts or retrieves the pid of the riak_dt_vnode for the given
%% partition index.
-spec start_vnode(partition()) -> {ok, pid()}.
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Retrieves the opaque value of the given data type and key.
%% Used inside request FSMs.
-spec value(riak_core_apl:preflist2(), module(), term(), term()) -> ok.
value(PrefList, Mod, Key, ReqId) ->
    riak_core_vnode_master:command(PrefList, {value, Mod, Key, ReqId}, {fsm, undefined, self()}, ?MASTER).

%% @doc Updates the value of the specified data type on this index.
-spec update(partition(), module(), term(), term()) -> ok.
update(IdxNode, Mod, Key, Args) ->
    ?sync(IdxNode, {update, Mod, Key, Args}, ?MASTER).

%% @doc Sends a state to the indexes in the preflist to merge with
%% their local states.
-spec merge(riak_core_apl:preflist2(), module(), term(), term(), term()) -> ok.
merge(PrefList, Mod, Key, CRDT, ReqId) ->
    riak_core_vnode_master:command(PrefList, {merge, Mod, Key, CRDT, ReqId}, {fsm, undefined, self()}, ?MASTER).

%% @doc Sends a read-repair of a value, which amounts to a merge with
%% no reply.
-spec repair(riak_core_apl:preflist2(), module(), term(), term()) -> ok.
repair(PrefList, Mod, Key, CRDT) ->
    riak_core_vnode_master:command(PrefList, {merge, Mod, Key, CRDT, ignore}, ignore, ?MASTER).

%% --------------------
%% riak_core_vnode API
%% --------------------

%% @doc Initializes the riak_dt_vnode.
-spec init([partition()]) -> {ok, #state{}}.
init([Partition]) ->
    Node = node(),
    VnodeId = uuid:v4(),
    StorageOptions = application:get_all_env(bitcask),
    {ok, DataDir, StorageState} = start_storage(Partition, StorageOptions),
    {ok, #state { partition=Partition,
                  node=Node,
                  storage_state=StorageState,
                  vnode_id=VnodeId,
                  data_dir=DataDir,
                  storage_opts=StorageOptions}}.

%% @doc Handles incoming vnode commands.
-spec handle_command(command(), sender(), #state{}) -> {noreply, #state{}} | {reply, term(), #state{}}.
handle_command({value, Mod, Key, ReqId}, Sender, #state{partition=Idx, node=Node, storage_state=StorageState}=State) ->
    lager:debug("value ~p ~p~n", [Mod, Key]),
    Reply = lookup({Mod, Key}, StorageState),
    riak_core_vnode:reply(Sender, {ReqId, {{Idx, Node}, Reply}}),
    {noreply, State};
handle_command({update, Mod, Key, Args}, _Sender, #state{storage_state=StorageState,vnode_id=VnodeId}=State) ->
    lager:debug("update ~p ~p ~p~n", [Mod, Key, Args]),
    Updated = case lookup({Mod, Key}, StorageState) of
                  {ok, {Mod, Val}} ->
                      Mod:update(Args, VnodeId, Val);
                  notfound ->
                      %% Not found, so create locally
                      Mod:update(Args, VnodeId, Mod:new());
                  {error, Reason} ->
                      {error, Reason}
              end,
    case Updated of
        {error, Reason2} ->
            lager:error("Error ~p looking up ~p.", [Reason2, {Mod, Key}]),
            {reply, {error, Reason2}, State};
        _ ->
            store({{Mod, Key}, {Mod, Updated}}, StorageState),
            {reply, {ok, {Mod, Updated}}, State}
    end;
handle_command({merge, Mod, Key, {Mod, RemoteVal}, ReqId}, Sender, #state{storage_state=StorageState}=State) ->
    lager:debug("Merge ~p ~p~n", [Mod, Key]),
    Reply = do_merge({Mod, Key}, RemoteVal, StorageState),
    riak_core_vnode:reply(Sender, {ReqId, Reply}),
    {noreply, State};
handle_command(merge_check, _Sender, #state{storage_state=StorageState,
                                            data_dir=DataDir,
                                            storage_opts=StorageOptions}=State) ->
    maybe_merge(DataDir, StorageOptions, StorageState),
    {noreply, State};
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

%% @doc Handles commands while in the handoff state.
-spec handle_handoff_command(vnode_req(), sender(), #state{}) -> {reply, term(), #state{}}.
handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State=#state{storage_state=StorageState}) ->
    Acc = fold(Fun, Acc0, StorageState),
    {reply, Acc, State}.

%% @doc Tells the vnode that handoff is starting.
-spec handoff_starting({partition(), node()}, #state{}) -> {true, #state{}}.
handoff_starting(_TargetNode, State) ->
    {true, State}.

%% @doc Tells the vnode that handoff was cancelled.
-spec handoff_cancelled(#state{}) -> {ok, #state{}}.
handoff_cancelled(State) ->
    {ok, State}.

%% @doc Tells the vnode that handoff finished.
-spec handoff_finished({partition, node()}, #state{}) -> {ok, #state{}}.
handoff_finished(_TargetNode, State) ->
    {ok, State}.

%% @doc Decodes and receives a handoff value from the previous owner.
%% For `riak_dt_vnode', the semantics of this is equivalent to a merge
%% command.
-spec handle_handoff_data(binary(), #state{}) -> {reply, ok, #state{}}.
handle_handoff_data(Binary, #state{storage_state=StorageState}=State) ->
    {KB, VB} = binary_to_term(Binary),
    {{Mod, Key}, {Mod, HoffVal}} = {binary_to_term(KB), binary_to_term(VB)},
    ok = do_merge({Mod, Key}, HoffVal, StorageState),
    {reply, ok, State}.

%% @doc Encodes a value to be sent over the wire in handoff.
-spec encode_handoff_item(key(), term()) -> binary().
encode_handoff_item(Name, Value) ->
    term_to_binary({Name, Value}).

%% @doc Determines whether this vnode is empty, that is, has no data.
-spec is_empty(#state{}) -> {true | false, #state{}}.
is_empty(State) ->
    {db_is_empty(State#state.storage_state), State}.

%% @doc Instructs the vnode to delete all its stored data.
-spec delete(#state{}) -> {ok, #state{}}.
delete(#state{storage_state=StorageState0, partition=Partition, storage_opts=Opts}=State) ->
    {ok, DataDir, StorageState} = drop_storage(StorageState0, Opts, Partition),
    {ok, State#state{storage_state=StorageState, data_dir=DataDir}}.

%% @doc Handles coverage requests.
-spec handle_coverage(vnode_req(), [{partition(), [partition()]}], sender(), #state{}) ->
    {stop, not_implemented, #state{}}.
handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

%% @doc Handles trapped exits from linked processes.
-spec handle_exit(pid(), term(), #state{}) -> {noreply, #state{}}.
handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

%% @doc Terminates the vnode.
-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, State) ->
    stop_storage(State#state.storage_state),
    ok.


%% -------------------
%% Internal functions
%% -------------------

%% @doc Performs a merge of a remote datatype into the local storage.
%% The semantics of this are that if the key does not exist, the
%% remote value will be accepted for the local state. If the key
%% exists, the datatype-specific merge function will be called with
%% the local and remote values and the merged value stored locally.
-spec do_merge(key(), term(), term()) -> ok | {error, term()}.
do_merge({Mod, Key}, RemoteVal, StorageState) ->
    Merged = case lookup({Mod, Key}, StorageState) of
                 notfound ->
                     RemoteVal;
                 {ok, {Mod, LocalValue}} ->
                     Mod:merge(LocalValue, RemoteVal);
                 {error, Reason} ->
                     {error, Reason}
             end,
    case Merged of
        {error, Reason2} ->
            lager:error("Looking up ~p failed with ~p, on merge.", [{Mod, Key}, Reason2]),
            ok;
        _ ->
            store({{Mod, Key}, {Mod, Merged}}, StorageState)
    end.


%% -------------------
%% Storage engine
%% -------------------

%% @doc Initializes the internal storage engine where datatypes are
%% persisted.
-spec start_storage(Partition :: integer(), Options::list()) ->
                           StorageState :: term().
start_storage(Partition, Options0) ->
    {ok, PartitionRoot} = get_data_dir(Partition),
    Options = set_mode(read_write, Options0),
    case bitcask:open(PartitionRoot, Options) of
        {error, Error} ->
            {error, Error};
        Cask ->
            schedule_merge(),
            {ok, PartitionRoot, Cask}
    end.

%% @doc Stops the internal storage engine.
-spec stop_storage(StorageState :: term()) ->
                           ok.
stop_storage(StorageState) ->
    bitcask:close(StorageState).


%% @doc Looks up a key in the persistent storage.
-spec lookup(K :: key(), StorageState :: term()) ->
                    {ok, {atom(), term()}} | notfound | {error, Reason :: term()}.
lookup(ModKey, StorageState) ->
    MKey = make_mkey(ModKey),
    case bitcask:get(StorageState, MKey) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        not_found ->
            notfound;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Persists a datatype under the given key.
-spec store({K :: key(), {atom(), term()}}, StorageState :: term()) ->
                   ok.
store({Key, Value}, StorageState) ->
    MKey = make_mkey(Key),
    bitcask:put(StorageState, MKey, term_to_binary(Value)).

%% @doc Folds over the persistent storage.
-spec fold(function(), list(), StorageState :: term()) ->
                  list().
fold(Fun, Acc, StorageState) ->
    bitcask:fold(StorageState, Fun, Acc).


%% @doc Determines whether the persistent storage is empty.
-spec db_is_empty(term()) -> boolean().
db_is_empty(StorageState) ->

    %% Taken, verabtim, from riak_kv_bitcask_backend.erl
    %% Determining if a bitcask is empty requires us to find at least
    %% one value that is NOT a tombstone. Accomplish this by doing a fold_keys
    %% that forcibly bails on the very first key encountered.
    F = fun(_K, _Acc0) ->
                throw(found_one_value)
        end,
    (catch bitcask:fold_keys(StorageState, F, undefined)) /= found_one_value.


%% @doc Drops the persistent storage, leaving no trace.
-spec drop_storage(term(),list(),integer()) -> reference().
drop_storage(StorageState, StorageOptions, Partition) ->
    %% Close the bitcask, delete the data directory
    ok = bitcask:close(StorageState),
    {ok, DataDir} = get_data_dir(Partition),
    {ok, Files} = file:list_dir(DataDir),
    [file:delete(filename:join([DataDir, F])) || F <- Files],
    ok = file:del_dir(DataDir),
    start_storage(Partition, StorageOptions).

%% @doc Creates a key appropriate for use in the persistent storage engine.
-spec make_mkey(key()) -> binary().
make_mkey(ModKey) ->
    term_to_binary(ModKey).

%% @doc Determines the root of the persistent storage engine for the
%% given partition.
-spec get_data_dir(integer()) ->
                                {ok, PartitionRoot :: string()}.
get_data_dir(Partition) ->
    DataRoot = app_helper:get_env(riak_dt, data_root, "data/riak_dt_bitcask"),
    PartitionRoot = filename:join(DataRoot, integer_to_list(Partition)),
    ok = filelib:ensure_dir(PartitionRoot),
    {ok, PartitionRoot}.


%% @doc Schedules a merge check and enqueue for a point in the future.
-spec schedule_merge() -> reference().
schedule_merge() ->
    riak_core_vnode:send_command_after(timer:minutes(3), merge_check).

%% @doc Checks whether a merge is necessary and enqueues it.
-spec maybe_merge(string(), list(), term()) -> ok.
maybe_merge(Root, Options, StorageState) ->
    case bitcask:needs_merge(StorageState) of
        {true, Files} ->
            bitcask_merge_worker:merge(Root, Options, Files);
        false ->
            ok
    end,
    schedule_merge(),
    ok.

%% @doc Sets the R/W mode, overriding any existing value and returning
%% a new configuration list.
-spec set_mode(read_only | read_write, list()) -> list().
set_mode(read_only, Config) ->
    Config1 = lists:keystore(read_only, 1, Config, {read_only, true}),
    lists:keydelete(read_write, 1, Config1);
set_mode(read_write, Config) ->
    Config1 = lists:keystore(read_write, 1, Config, {read_write, true}),
    lists:keydelete(read_only, 1, Config1).
