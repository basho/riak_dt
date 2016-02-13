-module(dict_compat_transform).

-export([parse_transform/2]).

-define(FAKEDICT, dict).
parse_transform(Forms, _Options) ->
  case has_maps() of
    true ->
      parse_trans:plain_transform(fun do_transform/1, Forms);
    false ->
      Forms
  end.

do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, new}}, []}) ->
  %[Body] = parse_trans:plain_transform(fun do_transform/1, [Body]),
  %{function,L1,store,Arity,Body};
  {call, L, {remote, L, {atom, L, maps}, {atom, L, new}}, []};

do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, fold}}, [Fun, Init, Val]}) ->
  [NewFun] = parse_trans:plain_transform(fun do_transform/1, [Fun]),
  [NewInit] = parse_trans:plain_transform(fun do_transform/1, [Init]),
  [NewVal] = parse_trans:plain_transform(fun do_transform/1, [Val]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, fold}}, [NewFun, NewInit, NewVal]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, find}}, [Key, MaybeMap]}) ->
  [NewKey] = parse_trans:plain_transform(fun do_transform/1, [Key]),
  [NewMaybeMap] = parse_trans:plain_transform(fun do_transform/1, [MaybeMap]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, find}}, [NewKey, NewMaybeMap]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, store}}, [Key, Value, Datastructure]}) ->
  [NewKey] = parse_trans:plain_transform(fun do_transform/1, [Key]),
  [NewValue] = parse_trans:plain_transform(fun do_transform/1, [Value]),
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, put}}, [NewKey, NewValue, NewDatastructure]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, erase}}, [Key, Datastructure]}) ->
  [NewKey] = parse_trans:plain_transform(fun do_transform/1, [Key]),
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, remove}}, [NewKey, NewDatastructure]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, update}}, [Key, Fun, Datastructure]}) ->
  [NewKey] = parse_trans:plain_transform(fun do_transform/1, [Key]),
  [NewFun] = parse_trans:plain_transform(fun do_transform/1, [Fun]),
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, dict_compat}, {atom, L, update}}, [NewKey, NewFun, NewDatastructure]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, update}}, [Key, Fun, Datastructure, Initial]}) ->
  [NewKey] = parse_trans:plain_transform(fun do_transform/1, [Key]),
  [NewFun] = parse_trans:plain_transform(fun do_transform/1, [Fun]),
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  [NewInitial] = parse_trans:plain_transform(fun do_transform/1, [Initial]),
  {call, L, {remote, L, {atom, L, dict_compat}, {atom, L, update}}, [NewKey, NewFun, NewDatastructure, NewInitial]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, fetch_keys}}, [Datastructure]}) ->
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, keys}}, [NewDatastructure]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, to_list}}, [Datastructure]}) ->
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, to_list}}, [NewDatastructure]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, size}}, [Datastructure]}) ->
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, size}}, [NewDatastructure]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, from_list}}, [Datastructure]}) ->
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, from_list}}, [NewDatastructure]};
do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, merge}}, [Fun, Datastructure1, Datastructure2]}) ->
  [NewFun] = parse_trans:plain_transform(fun do_transform/1, [Fun]),
  [NewDatastructure1] = parse_trans:plain_transform(fun do_transform/1, [Datastructure1]),
  [NewDatastructure2] = parse_trans:plain_transform(fun do_transform/1, [Datastructure2]),
  {call, L, {remote, L, {atom, L, dict_compat}, {atom, L, merge}}, [NewFun, NewDatastructure1, NewDatastructure2]};

do_transform({call, L, {remote, L, {atom, L, ?FAKEDICT}, {atom, L, map}}, [Fun, Datastructure]}) ->
  [NewFun] = parse_trans:plain_transform(fun do_transform/1, [Fun]),
  [NewDatastructure] = parse_trans:plain_transform(fun do_transform/1, [Datastructure]),
  {call, L, {remote, L, {atom, L, maps}, {atom, L, map}}, [NewFun, NewDatastructure]};
do_transform(_Form) ->
  %io:format("Visited Form: ~p~n", [Form]),
  continue.

% For debugging
%print_transform(Form) ->
%  io:format("Visited Form: ~w~n", [Form]),
%  continue.

has_maps() ->
  try maps:new() of
    _ ->
      true
  catch
    _ ->
      false
  end.