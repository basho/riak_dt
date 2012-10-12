-module(riak_dt_vnode_test).
-include_lib("eunit/include/eunit.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("riak_dt.hrl").
-compile([export_all]).


handoff_behavior_setup() ->
    filelib:ensure_dir("handoff_behavior_test/data/42"),
    application:set_env(riak_dt, data_root, "handoff_behavior_test/data"),
    {ok, State0, _Pool} = riak_dt_vnode:init([42]),
    {true, State} = riak_dt_vnode:handoff_starting('foo@bar.com', State0),
    State.

handoff_behavior_cleanup(State) ->
    riak_dt_vnode:delete(State).

handoff_behavior_test_() ->
    TestNames = [ verify_handoff_fold_local,
                  verify_handoff_update_local_and_forward_merge,
                  verify_handoff_merge_local_and_forward,
                  verify_handoff_value_local ],
    [ {setup,
       local,
       fun handoff_behavior_setup/0,
       fun handoff_behavior_cleanup/1,
       {with, [ fun ?MODULE:TestName/1 ]}
      } || TestName <- TestNames ].

%% Fold requests are handled locally, async
verify_handoff_fold_local(State) ->
    Folder = fun(_V,A) -> A end,
    Fold = riak_dt_vnode:handle_handoff_command(?FOLD_REQ{foldfun=Folder, acc0=[]}, ignore, State),
    ?assertMatch({async, {fold, _}, _, _}, Fold),
    {async, {fold, Work}, _, NS} = Fold,
    ?assertEqual([], Work()),
    NS.

verify_handoff_update_local_and_forward_merge(State) ->
    %% We use meck to detect the merge command to the future owner
    Key = <<"update_test">>,
    meck:new(riak_core_vnode_master),
    meck:expect(riak_core_vnode_master, command,
                fun({42,'foo@bar.com'}, 
                    ?MERGE_REQ{module=riak_dt_pncounter,
                               key=SentKey,
                               value={riak_dt_pncounter, _},
                               req_id=_}, ignore, riak_dt_vnode_master) ->
                        ?assertEqual(Key, SentKey),
                        ok
                end),
    %% We also use meck to detect the pre-emptive reply to the FSM
    meck:new(riak_core_vnode),
    meck:expect(riak_core_vnode, reply,
                fun(ignore, {ok, {riak_dt_pncounter, _}}) ->
                        ok
                end),
    Result = riak_dt_vnode:handle_handoff_command(
               ?UPDATE_REQ{module=riak_dt_pncounter,
                           key=Key,
                           args={increment, 5}},
               ignore,
               State),
    ?assertMatch({noreply, _}, Result),
    {noreply, NS} = Result,
    meck:unload(),
    NS.

%% Merges are applied both locally and forwarded
verify_handoff_merge_local_and_forward(State) ->
    Key = <<"merge_test">>,
    meck:new(riak_dt_vnode, [passthrough]),
    meck:expect(riak_dt_vnode, handle_command,
                fun(?MERGE_REQ{module=riak_dt_pncounter,
                               key=SentKey,
                               value={riak_dt_pncounter, _},
                               req_id=_}=Req,
                    ignore=Sender, MState) ->
                        ?assertEqual(Key, SentKey),
                        meck:passthrough([Req, Sender, MState])
                end),
    Value = riak_dt_pncounter:update({increment, 5}, 42, riak_dt_pncounter:new()),
    Results = riak_dt_vnode:handle_handoff_command(?MERGE_REQ{module=riak_dt_pncounter,
                                                              key=Key,
                                                              value={riak_dt_pncounter, Value},
                                                              req_id=0},
                                                   ignore, State),
    ?assertMatch({forward, _}, Results),
    {forward, NS} = Results,
    meck:unload(),
    NS.

%% Value requests are handled locally
verify_handoff_value_local(State) -> 
    Key = <<"value_test">>,
    meck:new(riak_dt_vnode, [passthrough]),
    meck:expect(riak_dt_vnode, handle_command,
                fun(?VALUE_REQ{module=riak_dt_pncounter,
                               key=SentKey,
                               req_id=0}=Req,
                    ignore=Sender, MState) ->
                        ?assertEqual(Key, SentKey),
                        meck:passthrough([Req, Sender, MState])
                end),
    Result = riak_dt_vnode:handle_handoff_command(?VALUE_REQ{module=riak_dt_pncounter,
                                                             key=Key,
                                                             req_id=0},
                                                  ignore, State),
    ?assertMatch({reply, {0, {{42, _Node}, notfound}}, _}, Result),
    {reply, _, NS} = Result,
    ?assert(true), 
    meck:unload(),
    NS.
