%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2011, Russell Brown
%%% @doc
%%% A behaviour for a state based CRDT
%%% @end
%%% Created : 22 Nov 2011 by Russell Brown <russelldb@basho.com>

-module(riak_dt).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{new, 0},
     {value, 1},
     {update, 3},
     {merge, 2},
     {equal, 2}].
