%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2012, Russell Brown
%%% @doc
%%%
%%% @end
%%% Created : 18 Jan 2012 by Russell Brown <russelldb@basho.com>

-module(counter_gen).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

gcounter() ->
    list(clock()).

pncounter() ->
    {gcounter(), gcounter()}.

clock() ->
    {int(), nat()}.

%%% Helpers
actor_count(Actor, Counter) ->
    case lists:keyfind(Actor, 1, Counter) of
        false ->
            0;
        {Actor, Value} -> Value
    end.

merged(C1, []) ->
    C1;
merged([], C2) ->
    C2;
merged(C1, C2) ->
    {M1, M2} = lists:mapfoldl(fun({A, V}, Acc) ->
                                      case lists:keytake(A, 1, Acc) of
                                          false ->
                                              {{A, V}, Acc};
                                          {value, {A, V2}, Rest}  ->
                                              {{A, max(V, V2)}, Rest}
                                      end
                              end,
                              C2, C1),
    M1++M2.
   


