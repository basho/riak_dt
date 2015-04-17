-module(map_size).

-compile([export_all]).

go() ->
    put(riak_dt_map, riak_dt_map:new()),
    put(riak_dt_map2, riak_dt_map2:new()),
    add_large_set_field(),
    [Ms, Ns] = replicate_and_add_element(),
    merge(Ms),
    merge(Ns),
    print_sizes(),
    add_element(),
    print_sizes().

add_large_set_field() ->
    Set = gen_set(),
    SetOp = {add_all, Set},
    [begin
         {ok, State2} = Mod:update({update, [{update, {<<"set">>, riak_dt_orswot}, SetOp}]}, crypto:rand_bytes(8), State),
         put(Mod, State2)
     end || {Mod, State} <- get()].

gen_set() ->
    [crypto:rand_bytes(N) || N <- [crypto:rand_uniform(10, 100) || _X <- lists:seq(1, crypto:rand_uniform(50, 200))]].

replicate_and_add_element() ->
    E = crypto:rand_bytes(10),
    [begin
         lists:map(fun(_) ->
                           {ok, State2} = add_element(E, Mod, State),
                           {Mod, State2}
                   end,
                   lists:seq(1, 3))
     end || {Mod, State} <- get()].


add_element() ->
    E = crypto:rand_bytes(10),
    [begin
         {ok, State2} = add_element(E, Mod, State),
         put(Mod, State2)
     end || {Mod, State} <- get()].

add_element(E, Mod, State) ->
    Mod:update({update, [{update, {<<"set">>, riak_dt_orswot}, {add, E}}]}, actor(), State).

actor() ->
    crypto:rand_bytes(8).

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
    [Size1, Size2] = [sizes(X) || X <- get()],
    io:format("~p~n~p~n", [Size1, Size2]).
