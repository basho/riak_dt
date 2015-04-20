-module(map_size).

-compile([export_all]).

%% Ugly hack on ugly hack. Why is this here? A random_seed tuple is
%% added to the process state when we invoke random:seed/1, and we
%% also use the process state to store the mods we want to test.
get_mods() ->
    lists:filter(fun({random_seed, _X}) -> false;
                    (_) -> true end,
                 get()).


%% Seed must be a 3-integer tuple
go(Mods, {_A, _B, _C}=Seed) ->
    erase(),
    lists:foreach(fun(Mod) -> put(Mod, Mod:new()) end,
                  Mods),
    random:seed(Seed),
    add_large_set_field(),
    lists:foreach(fun(List) -> merge(List) end,
                  replicate_and_add_element()),
    print_sizes(),
    add_element(),
    print_sizes().

rand_bytes(N) ->
    list_to_binary([random:uniform(255) || _X <- lists:seq(1, N)]).

add_large_set_field() ->
    Set = gen_set(),
    SetOp = {add_all, Set},
    [begin
         {ok, State2} = Mod:update({update, [{update, {<<"set">>, riak_dt_orswot}, SetOp}]}, rand_bytes(8), State),
         put(Mod, State2)
     end || {Mod, State} <- get_mods()].

gen_set() ->
    [rand_bytes(N) || N <- [10 + random:uniform(90) || _X <- lists:seq(1, 50 + random:uniform(150))]].

replicate_and_add_element() ->
    E = rand_bytes(10),
    [begin
         lists:map(fun(_) ->
                           {ok, State2} = add_element(E, Mod, State),
                           {Mod, State2}
                   end,
                   lists:seq(1, 3))
     end || {Mod, State} <- get_mods()].


add_element() ->
    E = rand_bytes(10),
    [begin
         {ok, State2} = add_element(E, Mod, State),
         put(Mod, State2)
     end || {Mod, State} <- get_mods()].

add_element(E, Mod, State) ->
    Mod:update({update, [{update, {<<"set">>, riak_dt_orswot}, {add, E}}]}, actor(), State).

actor() ->
    rand_bytes(8).

merge(L) ->
    {M, S} = lists:foldl(fun({Mod, State}, {Mod, Acc}) ->
                                 {Mod, Mod:merge(State, Acc)}
                         end,
                         hd(L),
                         tl(L)),
    put(M, S).

sizes({Mod, State}) ->
    {Mod, [{external, erlang:external_size(State)},
           {to_bin, byte_size(Mod:to_binary(State))}]}.

print_sizes() ->
    ListOfSizes = [sizes(X) || X <- get_mods()],
    io:format("~p~n", ListOfSizes).
