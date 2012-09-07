%% @doc Interface for riak_dt-admin commands.
-module(riak_dt_console).
-export([join/1,
         leave/1,
         remove/1,
         ringready/1]).

join([NodeStr]) ->
    try riak_core:join(NodeStr) of
        ok ->
            io:format("Sent join request to ~s\n", [NodeStr]),
            ok;
        {error, not_reachable} ->
            io:format("Node ~s is not reachable!\n", [NodeStr]),
            error;
        {error, different_ring_sizes} ->
            io:format("Failed: ~s has a different ring_creation_size~n",
                      [NodeStr]),
            error
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            io:format("Join failed, see log for details~n"),
            error
    end.

leave([]) ->
    remove_node(node()).

remove([Node]) ->
    remove_node(list_to_atom(Node)).

remove_node(Node) when is_atom(Node) ->
    try catch(riak_core:remove_from_cluster(Node)) of
        {'EXIT', {badarg, [{erlang, hd, [[]]}|_]}} ->
            %% This is a workaround because
            %% riak_core_gossip:remove_from_cluster doesn't check if
            %% the result of subtracting the current node from the
            %% cluster member list results in the empty list. When
            %% that code gets refactored this can probably go away.
            io:format("Leave failed, this node is the only member.~n"),
            error;
        Res ->
            io:format(" ~p\n", [Res])
    catch
        Exception:Reason ->
            lager:error("Leave failed ~p:~p", [Exception, Reason]),
            io:format("Leave failed, see log for details~n"),
            error
    end.

-spec(ringready([]) -> ok | error).
ringready([]) ->
    try riak_core_status:ringready() of
        {ok, Nodes} ->
            io:format("TRUE All nodes agree on the ring ~p\n", [Nodes]);
        {error, {different_owners, N1, N2}} ->
            io:format("FALSE Node ~p and ~p list different partition owners\n",
                      [N1, N2]),
            error;
        {error, {nodes_down, Down}} ->
            io:format("FALSE ~p down.  All nodes need to be up to check.\n",
                      [Down]),
            error
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception, Reason]),
            io:format("Ringready failed, see log for details~n"),
            error
    end.
