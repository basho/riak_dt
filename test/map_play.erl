-module(map_play).

-compile([export_all]).


flags(MapMod) ->
    F = field(<<"flag">>, riak_dt_od_flag),
    {ok, A} = MapMod:update(update_op(F, enable), a, MapMod:new()),
    ACtx = MapMod:precondition_context(A),
    {ok, B} = MapMod:update(update_op(F, disable), b, MapMod:new(), ACtx),
    BCtx = MapMod:precondition_context(B),
    {ok, A2} = MapMod:update(remove(F), a, A, BCtx),
    map_out("A", MapMod, A),
    map_out("B", MapMod, B),
    map_out("A2", MapMod, A2),
    AB = MapMod:merge(A2, B),
    map_out("AB", MapMod, AB).

registers(MapMod, RegMod) ->
    F = reg_field(RegMod),
    Op = {assign, <<"bob">>, 3},
    Update = update_op(F, Op),
    {ok, A} = MapMod:update(Update, a, MapMod:new()),
    {ok, B} = MapMod:update(update_op(F, {assign, <<"bill">>, 2}), b, MapMod:new()),
    out("A Reg ~p~n", [MapMod:value(A)]),
    out("B Reg ~p~n", [MapMod:value(B)]),
    AB = MapMod:merge(A, B),
    out("AB map~p~n", [MapMod:to_version(1, AB)]),
    out("AB reg ~p~n", [MapMod:value(AB)]),
    {ok, A2} = MapMod:update({update, [{remove, F}]}, a, A),
    out("A2 reg ~p~n", [MapMod:value(A2)]),
    AB2 = MapMod:merge(A2, AB),
    out("AB2 reg ~p~n", [MapMod:value(AB2)]),
    %% Update with past
    {ok, ABWut} = MapMod:update(update_op(F, {assign, <<"phil">>, 1}), a, AB2),
    out("Past?? ~p~n~p~n", [MapMod:value(ABWut), MapMod:to_version(1, ABWut)]).

counters(MapMod, CounterMod) ->
    F = counter_field(CounterMod),
    Op = {increment, 1},
    Update = update_op(F, Op),
    {ok, A} = MapMod:update(Update, a, MapMod:new()),
    {ok, A2} = MapMod:update(Update, a, A),
    B = A2,
    {ok, A3} = MapMod:update({update, [{remove, F}]}, a, A2),
    {ok, A4} = MapMod:update(Update, a, A3),
    out("A counter ~p~n", [MapMod:value(A4)]),
    AB = MapMod:merge(A4, B),
    out("AB counter ~p~n", [MapMod:value(AB)]),
    {ok, B2} = MapMod:update(Update, b, B),
    out("B counter ~p~n", [MapMod:value(B2)]),
    AB2 = MapMod:merge(A4, B2),
    out("AB2 counter ~p~n", [MapMod:value(AB2)]).

reg_field(Mod) ->
    field(<<"reg">>, Mod).

counter_field(Mod) ->
    field(<<"counter">>, Mod).

field(Name, Mod) ->
    {Name, Mod}.

update_op(Field, Op) ->
    {update, [{update, Field, Op}]}.

remove(F) ->
    {update, [{remove, F}]}.

map_out(Label, MapMod, Map) ->
    Format = Label ++ " ~p~n~p~n",
    Args = [MapMod:to_v1(Map), MapMod:value(Map)],
    out(Format, Args).

out(Format, Args) ->
    io:format(Format, Args).
