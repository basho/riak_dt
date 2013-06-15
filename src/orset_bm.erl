-module(orset_bm).

-compile([export_all]).

%% Quick'n'dirty micro benchmark of OR-Set vs. VVOR-Set for size of garbage
%% after a round of flappy add / remove of the same items

%% Start a process for managing the test
%% Create N named processes
%% Each process creates a new or-set
%% each process runs N operations
%% Op is one of add, remove, merge
%% Add and remove pick a random item from
%% a set of K values (or from the Added set)
%% Merge picks another process at random to merge with
%% At test end all processes send there final value to
%% the co-ordindator
%% The co-ordinator merges all values and gives a total size
-define(N, 10).

start(Cmds, Mod) ->
    TS = erlang:now(),
    Coord =  start_coordinator(?N, Mod, self()),
    [start_proc(I, Cmds, Mod, Coord) || I <- lists:seq(1, ?N)],

    receive
        {Coord, Results} ->
            TE = erlang:now(),
            Size = m_size(Mod, Results),
            io:format("Results are ~p in ~p~n", [Size, timer:now_diff(TE, TS) / 1000])
    end.


m_size(Mod, Results) ->
    Values = Mod:value(Results),
    {length(Values), byte_size(term_to_binary(Results))}.

%% Coordinator
start_coordinator(N, Mod, Top) ->
    spawn(fun() ->
                  coord_loop(N, Mod, Mod:new(), Top)
          end).

coord_loop(0, _Mod, Merged, Top) ->
    Top ! {self(), Merged};
coord_loop(N, Mod, Merged, Top) ->
    receive
        Val ->
            coord_loop(N-1, Mod, Mod:merge(Val, Merged), Top)
    end.

%% Workers
start_proc(I, Cmds, Mod, Coord) ->
    Pid = spawn(fun() -> proc_loop(Cmds, Coord, {I, Mod, Mod:new(), ordsets:new()}) end),
    register(list_to_atom(integer_to_list(I)), Pid).

proc_loop(0, Coord, {_I, _Mod, Val, _AL}) ->
    Coord ! Val;
proc_loop(Cmds, Coord, State0) ->
    %% Choose an action at random
    Action = select_action(crypto:rand_uniform(1, 1000)),
    %% perform action
    {I, Mod, Val0, AL} = perform(Action, State0),
    Val = receive
              {merge, Peer, Remote} ->
                  Peer ! Mod:merge(Remote, Val0)
          after 0 ->
                  Val0
          end,
    %% Loop
    State = maybe_merge({I, Mod, Val, AL}, crypto:rand_uniform(1 ,100)),
    proc_loop(Cmds - 1, Coord, State).

select_action(N) when N > 1 ->
    add;
select_action(1)  ->
    remove.

maybe_merge(State, 1) ->
    perform(merge, State);
maybe_merge(State, _N) ->
    State.

perform(add, {I, Mod, Val0, AL0}) ->
    Elem = crypto:rand_uniform(1, 100),
    Val = Mod:update({add, Elem}, <<I:64>>, Val0),
    AL = ordsets:add_element(Elem, AL0),
    {I, Mod, Val, AL};
perform(remove, {I, Mod, Val0, AL0}) ->
    case ordsets:size(AL0) of
        Small when Small < 2 ->
            {I, Mod, Val0, AL0};
        Size ->
            Idx = crypto:rand_uniform(1, Size),
            Elem = lists:nth(Idx, ordsets:to_list(AL0)),
            Val = Mod:update({remove, Elem}, <<I:64>>, Val0),
            AL = ordsets:del_element(Elem, AL0),
            {I, Mod, Val, AL}
    end;
perform(merge, {I, Mod, Val0, AL0}) ->
    Peer = crypto:rand_uniform(1, ?N),
    PeerName = list_to_atom(integer_to_list(Peer)),
    case whereis(PeerName) of
        Pid when is_pid(Pid) ->
            Pid ! {merge, self(),  Val0};
        _ -> ok
    end,
    {I, Mod, Val0, AL0}.





