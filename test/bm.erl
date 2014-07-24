-module(bm).

-compile(export_all).

test_avg(N) when N > 0 ->
    {SetSizes, RawSizes, TCTimes, CPUTimes} = test_loop(N, {[], [], [], []}),
    TCHisto = histo(TCTimes, N),
    SetSizeHisto = histo(SetSizes, N),
    RawSizeHisto = histo(RawSizes, N),
    CPUHisto = histo(CPUTimes, N),
    %%    io:format("Times: ~p~nSize: ~p~n", [THisto, SHisto]),
    {{tc, TCHisto}, {cpu, CPUHisto}, {size, SetSizeHisto}, {rawsize, RawSizeHisto}}.

test_loop(0, Results) ->
    Results;
test_loop(N, {SetSizes0, RawSizes0, TCTimes0, CPUTimes0}) ->
    %% Should use both timer:tc and erlang:statistics(runtime) here
    %% Also, should spawn a process _just_ for the test
    Boss = self(),
    Actor = crypto:rand_bytes(8),
    Element = crypto:rand_bytes(20),
    Base = riak_dt_orswot:new(),
    Tester = spawn(fun() ->
                           {T, {ok, Set}} = timer:tc(riak_dt_orswot, update, [{add, Element}, Actor, Base]),
                           _ = erlang:statistics(runtime),
                           _ = riak_dt_orswot:update({add, Element}, Actor, Base),
                           {_, CPUTime} = erlang:statistics(runtime),
                           Values = riak_dt_orswot:value(Set),
                           SetSize = byte_size(riak_dt_orswot:to_binary(Set)),
                           RawSize = byte_size(term_to_binary(Values, [{compressed, 1}])),
                           Boss ! {self(), {T, CPUTime, SetSize, RawSize}}
                   end),
    {T, CPUTime, SetSize, RawSize} = receive
                                         {Tester, Res} ->
                                             Res
                                     end,

    SetSizes = [SetSize | SetSizes0],
    RawSizes = [RawSize | RawSizes0],
    TCTimes = [T|TCTimes0],
    CPUTimes = [CPUTime | CPUTimes0],
    test_loop(N - 1, {SetSizes, RawSizes, TCTimes, CPUTimes}).

empty_set_n_actors(N) ->
    %% Add, and remove an element per actor, leaves an empty set with
    %% an entry per actor
    Set = lists:foldl(fun(Actor, SetAcc) ->
                             {ok, Added} = riak_dt_orswot:update({add, 1}, Actor, SetAcc),
                             {ok, Removed} = riak_dt_orswot:update({remove, 1}, Actor, Added),
                             Removed
                      end,
                      riak_dt_orswot:new(),
                      gen_actors(N)),
    sizes(riak_dt_orswot, Set).

%% @doc generate a set with `EntriesN' elements and `ActorsN'
%% Actors. Each element is unique, each actor is unique. Actors take
%% it in turns, round robin, to add elements.
%% `ElementsN' Actors will be present!
n_actors_n_entries(ActorsN, EntriesN, Type) when ActorsN < 1;
                                                 EntriesN < 1;
                                                 Type /= byte andalso Type /= int ->
    error(badarg, [ActorsN, EntriesN, Type]);
n_actors_n_entries(ActorsN, EntriesN, Type) when EntriesN >= ActorsN ->
    Actors = gen_actors(ActorsN),
    Entries = gen_entries(Type, EntriesN),
    {Set, _FinalCnt} = lists:foldl(fun(Entry, {SetAcc, Cnt}) ->
                                           Actor = round_robin(Actors, Cnt),
                                           {ok, Added} = riak_dt_orswot:update({add, Entry}, Actor, SetAcc),
                                           {Added, Cnt+1}
                                   end,
                                   {riak_dt_orswot:new(), 0},
                                   Entries),
    sizes(riak_dt_orswot, Set);
n_actors_n_entries(ActorsN, EntriesN, Type) ->
    Actors = gen_actors(ActorsN),
    Entries = gen_entries(Type, EntriesN),
    {Set, _FinalCnt} = lists:foldl(fun(Actor, {SetAcc, Cnt}) ->
                                           Entry = round_robin(Entries, Cnt),
                                           {ok, Added} = riak_dt_orswot:update({add, Entry}, Actor, SetAcc),
                                           {Added, Cnt+1}
                                   end,
                                   {riak_dt_orswot:new(), 0},
                                   Actors),
    sizes(riak_dt_orswot, Set).

%% @doc each of `ActorsN' actors adds `EntriesN' elements to a set,
%% and we merge all sets. This creates the largest possible "minimal
%% clock" per entry, so the worst case set size. The final Set will
%% always have `ActorsN' Actors and `EntriesN' entries.
every_actor_every_element(ActorsN, EntriesN, Type) when ActorsN < 1;
                                                        EntriesN < 1;
                                                        Type /= byte andalso Type /= int ->
    error(badarg, [ActorsN, EntriesN, Type]);
every_actor_every_element(ActorsN, EntriesN, Type) ->
    Actors = gen_actors(ActorsN),
    Entries = gen_entries(Type, EntriesN),
    Set = lists:foldl(fun(Actor, Merged) ->
                              Set = lists:foldl(fun(Entry, SetAcc) ->
                                                        {ok, Added} = riak_dt_orswot:update({add, Entry}, Actor, SetAcc),
                                                        Added
                                                end,
                                                riak_dt_orswot:new(),
                                                Entries),
                              riak_dt_orswot:merge(Set, Merged)
                      end,
                      riak_dt_orswot:new(),
                      Actors),
    sizes(riak_dt_orswot, Set).

round_robin([], Cnt) ->
    error(badarg, [[], Cnt]);
round_robin(L, N) ->
    lists:nth((N rem length(L)) + 1, L).

%% @doc for a dt `Mod' and `Val' that is a CRDT for that `Mod', return
%% a two tuple `{Size, RawSize}' where `Size' is the size in bytes of
%% the wire/disk binary representation of the `Val' for `Mod' (from
%% riak_dt:to_binary/1) and `RawSize' is the result of calling
%% `term_to_binary/2' on the result of calling `value/1' on `Mod' with
%% `Val' as an argument. This an attempt to figure out what the
%% byte_size of just the stored data would be without the
%% implementation overhead. For example, a counter would be a single
%% int vs. a vector of actor->int mappings. A Set would be a list of
%% the elements, vs. a VV+clock->element mapping.
sizes(Mod, Val) ->
    Vals = Mod:value(Val),
    BinSize = byte_size(Mod:to_binary(Val)),
    RawSize = byte_size(term_to_binary(Vals, [{compressed, 1}])),
    %%io:format("~p~n", [Mod:stats(Val)]),
    {BinSize, RawSize}.

gen_entries(byte, N) ->
    gen_actors(N);
gen_entries(int, N) ->
    lists:seq(1, N).

gen_actors(N) ->
    gen_actors(N, []).

gen_actors(N, Actors) when N == length(Actors) ->
    Actors;
gen_actors(N, Actors) ->
    Actors2 = add_actor(Actors),
    gen_actors(N, Actors2).

add_actor(Actors) ->
    Actor = crypto:rand_bytes(8),
    case lists:member(Actor, Actors) of
        true ->
            add_actor(Actors);
        false ->
            [Actor | Actors]
    end.


histo(L0, N) ->
    L = lists:sort(L0),
    Histo = [{Name, kth(K, L, N)} || {Name, K} <-
                                         [{"50th", 50},
                                          {"90th", 90},
                                          {"95th", 95},
                                          {"99th", 99},
                                          {"max", 100}]],
    [[{"Avg", avg(L, N)}, {"Min", hd(L)}] | Histo].

avg(L, N) ->
    lists:sum(L) / N.

kth(K, L, N) ->
    Index = (K/100) * N,
    RIndex = round(Index),
    case RIndex == Index of
        true ->
            Sub = lists:sublist(L, RIndex, 2),
            lists:sum(Sub)  / length(Sub);
        false ->
            lists:nth(RIndex, L)
    end.
