%%%-------------------------------------------------------------------
%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% kicks off riak_crdt_update_fsms on a 141 basis
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>
%%%-------------------------------------------------------------------
-module(riak_crdt_update_fsm_sup).
-behaviour(supervisor).

-export([start_update_fsm/2]).
-export([start_link/0]).
-export([init/1]).

start_update_fsm(Node, Args) ->
    supervisor:start_child({?MODULE, Node}, Args).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    UpdateFsmSpec = {undefined,
               {riak_crdt_update_fsm, start_link, []},
               temporary, 5000, worker, [riak_crdt_update_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [UpdateFsmSpec]}}.
