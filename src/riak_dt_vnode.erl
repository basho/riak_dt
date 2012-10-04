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

-type command() :: ?VALUE_REQ{} | ?UPDATE_REQ{} | ?MERGE_REQ{}.

-record(state, {
          partition :: partition(),
          node :: node(),
          storage_state :: term(),
          vnode_id :: binary(),
          data_dir :: file:filename(),
          storage_opts :: [ term() ],
          handoff_target :: node()}).

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
    random:seed(erlang:now()), %% In this case we _DO_ want monotonicity
    VnodeId = uuid:v4(),
    StorageOptions = application:get_all_env(bitcask),
    {ok, DataDir, StorageState} = start_storage(Partition, StorageOptions),
    PoolSize = app_helper:get_env(riak_dt, worker_pool_size, 10),
    {ok, #state { partition=Partition,
                  node=Node,
                  storage_state=StorageState,
                  vnode_id=VnodeId,
                  data_dir=DataDir,
                  storage_opts=StorageOptions},
     [{pool, riak_dt_vnode_worker, PoolSize, []}]
    }.

%% @doc Handles incoming vnode commands.
-spec handle_command(command(), sender(), #state{}) ->
                            {noreply, #state{}} | {reply, term(), #state{}}.

handle_command(?VALUE_REQ{module=Mod, key=Key, req_id=ReqId}, _Sender,
               #state{partition=Idx, node=Node, storage_state=StorageState}=State) ->
    %% Value requests are like KV's "get" request, they return the
    %% value of the data-structure that this vnode has stored.
    lager:debug("value ~p ~p~n", [Mod, Key]),
    Reply = lookup({Mod, Key}, StorageState),
    {reply, {ReqId, {{Idx, Node}, Reply}}, State};

handle_command(?UPDATE_REQ{module=Mod, key=Key, args=Args}, _Sender,
               #state{storage_state=StorageState,vnode_id=VnodeId}=State) ->
    %% Update requests apply the requested operation given by Args to
    %% the locally-stored representation of the data-structure and
    %% return the updated value to the requesting FSM.
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

handle_command(?MERGE_REQ{module=Mod, key=Key, value={Mod, RemoteVal}, req_id=ReqId},
               _Sender, #state{storage_state=StorageState}=State) ->
    %% Merge requests apply the state of a remote replica to the local
    %% replica's state, using the merge function defined by the
    %% data-structure.
    lager:debug("Merge ~p ~p~n", [Mod, Key]),
    Reply = do_merge({Mod, Key}, RemoteVal, StorageState),
    {reply, {ReqId, Reply}, State};

handle_command(merge_check, _Sender, #state{storage_state=StorageState,
                                            data_dir=DataDir,
                                            storage_opts=StorageOptions}=State) ->
    %% This is a callback to check and possibly invoke bitcask
    %% compaction (not to be confused with merge requests).
    maybe_merge(DataDir, StorageOptions, StorageState),
    {noreply, State};

handle_command(Message, _Sender, #state{partition=P}=State) ->
    %% Unknown commands are ignored
    lager:error("~p:~p received unhandled command: ~p.", [?MODULE, P, Message]),
    {noreply, State}.

%% @doc Handles commands while in the handoff state.
-spec handle_handoff_command(vnode_req(), sender(), #state{}) -> {reply, term(), #state{}}.
handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
                       State=#state{storage_opts=Opts, data_dir=DataDir}) ->
    %% This should be the handoff fold for sending data to the new
    %% owner. We perform it async using the worker pool, allowing
    %% other requests to complete while handoff is happening. Since we
    %% currently don't reply to folds in any other place, we don't
    %% delegate this to handle_command.
    {async, Work} = async_fold(Fun, Acc0, DataDir, Opts),
    {async, {fold, Work}, Sender, State};

handle_handoff_command(?UPDATE_REQ{key=Key}=Req, Sender,
                       #state{partition=Partition,
                              handoff_target=Target}=State) ->
    %% Updates are sent only to primaries, so in this case we must be
    %% in ownership handoff. One of the parties in handoff needs to
    %% perform the update. We opt to apply the update locally, on the
    %% current owner, and then send a merge to the future owner in
    %% case the previous value has not been handed off yet.
    case handle_command(Req, Sender, State) of
        {reply, {error, _Reason}, _NS}=Reply ->
            Reply;
        {reply, {ok, {Mod, Updated}}=Reply, NS} ->
            riak_core_vnode:reply(Sender, Reply),
            riak_core_vnode_master:command({Partition, Target},
                                           ?MERGE_REQ{module=Mod,
                                                      key=Key,
                                                      value={Mod, Updated}},
                                           ignore, ?MASTER),
            {noreply, NS}
    end;

handle_handoff_command(?MERGE_REQ{}=Req, Sender, State) ->
    %% While in handoff, merges are taken locally so that the local
    %% state gets updated just-in-case, but also forwarded in case
    %% that value has already been handed off.
    handle_command(Req, Sender, State),
    {forward, State};

handle_handoff_command(Req, Sender, State) ->
    %% Value requests can always be serviced by the current owner
    %% vnode. All other requests are ignored, as they are in
    %% handle_command.
    handle_command(Req, Sender, State).


%% @doc Tells the vnode that handoff is starting.
-spec handoff_starting({partition(), node()}, #state{}) -> {true, #state{}}.
handoff_starting(TargetNode, State) ->
    %% We track who we are handing off to so that we can forward local
    %% updates as merges to the future owner.
    {true, State#state{handoff_target=TargetNode}}.

%% @doc Tells the vnode that handoff was cancelled.
-spec handoff_cancelled(#state{}) -> {ok, #state{}}.
handoff_cancelled(State) ->
    %% We're no longer handing off, so the target node is cleared.
    {ok, State#state{handoff_target=undefined}}.

%% @doc Tells the vnode that handoff finished.
-spec handoff_finished({partition, node()}, #state{}) -> {ok, #state{}}.
handoff_finished(_TargetNode, State) ->
    %% Handoff finished and we're about to enter the forwarding state,
    %% so we can unset the target node.
    {ok, State#state{handoff_target=undefined}}.

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
%% -spec fold(function(), list(), StorageState :: term()) ->
%%                   list().
%% fold(Fun, Acc, StorageState) ->
%%     bitcask:fold(StorageState, Fun, Acc).


%% @doc Folds over the persistent storage using the worker pool.
async_fold(Fun, Acc, DataDir, StorageOpts) ->
    ReadOpts = set_mode(read_only, StorageOpts),
    Folder = fun() ->
                     case bitcask:open(DataDir, ReadOpts) of
                         Ref1 when is_reference(Ref1) ->
                             try
                                 bitcask:fold(Ref1, Fun, Acc)
                             after
                                 bitcask:close(Ref1)
                             end;
                         {error, Reason} -> {error, Reason}
                     end
             end,
    {async, Folder}.


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
    ok = filelib:ensure_dir(filename:join(PartitionRoot, ".dummy")),
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
