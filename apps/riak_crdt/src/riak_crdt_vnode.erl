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

-record(state, {partition, node, table}).

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
    Table = "data/" ++ integer_to_list(Partition),
    {ok, Table} = dets:open_file(Table, []),
    {ok, #state { partition=Partition, node=Node, table=Table }}.

handle_command({value, Mod, Key, ReqId}, Sender, #state{partition=Idx, node=Node, table=Table}=State) ->
    lager:debug("value ~p ~p~n", [Mod, Key]),
    Reply = case dets:lookup(Table, {Mod, Key}) of
                [{{Mod, Key}, {Mod, Val}}] -> {Mod, Val};
                [] -> notfound
            end,
    riak_core_vnode:reply(Sender, {ReqId, {{Idx, Node}, Reply}}),
    {noreply, State};
handle_command({update, Mod, Key, Args}, _Sender, #state{table=Table, partition=Idx}=State) ->
    lager:debug("update ~p ~p ~p~n", [Mod, Key, Args]),
    Reply = case dets:lookup(Table, {Mod, Key}) of
                [{{Mod, Key}, {Mod, Val}}] -> 
                    Updated = Mod:update(Args, {node(), Idx}, Val),
                    ok = dets:insert(Table, {{Mod, Key}, {Mod, Updated}}),
                    {ok, {Mod, Updated}};
                [] ->
                    %% Not found, so create locally
                    Updated = Mod:update(Args, {node(), Idx}, Mod:new()),
                    ok = dets:insert(Table, {{Mod, Key}, {Mod, Updated}}),
                    {ok, {Mod, Updated}}
            end,
    {reply, Reply, State};
handle_command({merge, Mod, Key, {Mod, RemoteVal} = Remote, ReqId}, Sender, #state{table=Table}=State) ->
    lager:debug("Merge ~p ~p~n", [Mod, Key]),
    Reply = case dets:lookup(Table, {Mod, Key}) of
                [{{Mod, Key}, {Mod, LocalVal}}] ->
                    Merged = Mod:merge(LocalVal, RemoteVal),
                    dets:insert(Table, {{Mod, Key}, {Mod, Merged}});
                [] ->
                    dets:insert(Table, {{Mod, Key}, Remote})
            end,
    riak_core_vnode:reply(Sender, {ReqId, Reply}),
    {noreply, State};
handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State=#state{table=Table}) ->
    F = fun({K, V}, AccIn) -> Fun(K, V, AccIn) end,
    Acc = dets:foldl(F, Acc0, Table),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Binary, #state{table=Table}=State) ->
    {{Mod, Key}, {Mod, HoffVal}} = binary_to_term(Binary),
    %% merge with local
     case dets:lookup(Table, {Mod, Key}) of
         [{{Mod, Key}, {Mod, LocalVal}}] ->
             Merged = Mod:merge(LocalVal, HoffVal),
             dets:insert(Table, {{Mod, Key}, {Mod, Merged}});
         [] ->
             dets:insert(Table, {{Mod, Key}, {Mod, HoffVal}})
         end,
    {reply, ok, State}.

encode_handoff_item(Name, Value) ->
    term_to_binary({Name, Value}).

is_empty(State) ->
    case dets:info(State#state.table, no_keys) of
        0 -> {true, State};
        _ -> {false, State}
    end.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
