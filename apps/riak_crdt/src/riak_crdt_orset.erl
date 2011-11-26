%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% an OR-Set CRDT
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>

-module(riak_crdt_orset).

-behaviour(riak_crdt).

%% API
-export([new/0, value/1, update/3, merge/2, equal/2]).

new() ->
    {dict:new(), dict:new()}.

value({ADict, RDict}) ->
    dict:fetch_keys(dict:filter(fun(K, V) ->
                                        case dict:find(K, RDict) of 
                                            {ok, RSet} ->
                                                case
                                                    sets:to_list(sets:subtract(V, RSet)) of
                                                    [] -> false;
                                                    _ -> true
                                                end;
                                            error -> true
                                        end
                                end,
                                ADict)).

update({add, Elem}, Actor, {ADict0, RDict}) ->
    ADict = add_elem(Actor, ADict0, Elem),
    {ADict, RDict};
update({remove, Elem}, _Actor, {ADict, RDict0}) ->
    RDict = remove_elem(dict:find(Elem, ADict), Elem, RDict0),
    {ADict, RDict}.
merge({ADict1, RDict1}, {ADict2, RDict2}) ->   
    MergedADict = merge_dicts(ADict1, ADict2),
    MergedRDict = merge_dicts(RDict1, RDict2),
    {MergedADict, MergedRDict}.

equal({ADict1, RDict1}, {ADict2, RDict2}) ->
    ADict1 =:= ADict2 andalso RDict1 =:= RDict2.

%% Private
add_elem(Actor, Dict, Elem) ->
    Unique = unique(Actor),
    add_unique(dict:find(Elem, Dict), Dict, Elem, Unique).

remove_elem({ok, Set0}, Elem, Dict) ->
    case dict:find(Elem, Dict) of
        {ok, Set} ->
            dict:store(Elem, sets:union(Set, Set0), Dict);
        error ->
            dict:store(Elem, Set0, Dict)
    end.

add_unique({ok, Set0}, Dict, Elem, Unique) ->
    Set = sets:add_element(Unique, Set0),
    dict:store(Elem, Set, Dict);
add_unique(error, Dict, Elem, Unique) ->
    Set = sets:from_list([Unique]),
    dict:store(Elem, Set, Dict).

unique(Actor) ->
    erlang:phash2({Actor, erlang:now()}).

merge_dicts(Dict1, Dict2) ->
    %% for every key in dict1, merge its contents with dict2's content for same key
   dict:merge(fun(_K, V1, V2) -> sets:union(V1, V2) end, Dict1, Dict2).
