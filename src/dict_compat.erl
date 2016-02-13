%%%-------------------------------------------------------------------
%%% @author sdhillon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Feb 2016 12:31 AM
%%%-------------------------------------------------------------------
-module(dict_compat).
-author("sdhillon").

-export([update/3, update/4, merge/3]).

update(Key, Fun, Map) ->
  Value = map:get(Key, Map),
  Value1 = Fun(Value),
  maps:put(Key, Value1, Map).

update(Key, Fun, Initial, Map) ->
  Value = map:get(Key, Map, Initial),
  Value1 = Fun(Value),
  maps:put(Key, Value1, Map).

merge(Fun, Map1, Map2) ->
  Map1KeysSet = ordsets:from_list(maps:keys(Map1)),
  Map2KeysSet = ordsets:from_list(maps:keys(Map2)),
  KeysToMerge = ordsets:intersection(Map1KeysSet, Map2KeysSet),
  KeysToAddFromMap1 = ordsets:subtract(Map1KeysSet, KeysToMerge),
  KeysToAddFromMap2 = ordsets:subtract(Map2KeysSet, KeysToMerge),
  Map1Subset = maps:with(KeysToAddFromMap1, Map1),
  Map2Subset = maps:with(KeysToAddFromMap2, Map2),
  CombinedMap = maps:merge(Map1Subset, Map2Subset),
  {_, _, CombinedMap1} =
    lists:foldl(merge2(Fun), {Map1, Map2, CombinedMap}, KeysToMerge),
  CombinedMap1.

merge2(Fun) ->
  fun(Key, {Map1, Map2, CombinedMap}) ->
    Map1Value = maps:get(Key, Map1),
    Map2Value = maps:get(Key, Map2),
    NewValue = Fun(Key, Map1Value, Map2Value),
    CombinedMap1 = maps:put(Key, NewValue, CombinedMap),
    {Map1, Map2, CombinedMap1}
  end.





