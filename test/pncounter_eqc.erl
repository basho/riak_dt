%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2012, Russell Brown
%%% @doc
%%% basic tests of counter crdts
%%% @end
%%% Created : 18 Jan 2012 by Russell Brown <russelldb@basho.com>

-module(pncounter_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(NUMTESTS, 1000).
-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) ->
                              io:format(user, Str, Args) end, P)).

%% eqc_value_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_value()))).

%% eqc_increment_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_update(increment, 1, 1)))).

%% eqc_increment_by_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_update_by(increment, 1, 1)))).

%% eqc_decrement_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_update(decrement, 2, -1)))).

%% eqc_decrement_by_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_update_by(decrement, 2, -1)))).

%% eqc_merge_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_merge()))).

%% eqc_equal_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_equal()))).

prop_value() ->
    ?FORALL(Counter, counter_gen:pncounter(),
            equals(riak_crdt_pncounter:value(Counter), 
                   total(Counter))).

prop_update_by(Action, Elem, Diff) ->
    ?FORALL({Counter, Actor, By}, {counter_gen:pncounter(), int(), int()},
            collect(By < 1, 
                    begin
                        case catch( riak_crdt_pncounter:update({Action, By}, Actor, Counter) )  of
                            {'EXIT',{function_clause,[{riak_crdt_pncounter,update,[{Action, By}, Actor, Counter]}| _]}} ->
                                By < 1;
                            Counter2 ->
                                conjunction([{actor_count, equals(counter_gen:actor_count(Actor, element(Elem, Counter2)), 
                                                                  (counter_gen:actor_count(Actor, element(Elem, Counter)) + By))},
                                             {total, equals(riak_crdt_pncounter:value(Counter2), 
                                                            total(Counter) + (Diff * By))}])
                        end
                    end
                   )).
    

prop_update(Action, Elem, Diff) ->
    ?FORALL({Counter, Actor}, {counter_gen:pncounter(), int()},
            begin
                Counter2 = riak_crdt_pncounter:update(Action, Actor, Counter),
                conjunction([{actor_count, equals(counter_gen:actor_count(Actor, element(Elem, Counter2)),
                                                  (counter_gen:actor_count(Actor, element(Elem, Counter)) +1))},
                             {total, equals(riak_crdt_pncounter:value(Counter2), 
                                            total(Counter) + (1 * Diff))}])
            end
           ).

prop_merge() ->
    ?FORALL({{Plus1, Minus1}=Counter1, {Plus2, Minus2}=Counter2}, {counter_gen:pncounter(), counter_gen:pncounter()},
            begin
                {Plus12, Minus12} = riak_crdt_pncounter:merge(Counter1, Counter2),
                conjunction([{plus_eq, equals(lists:sort(Plus12),
                                              lists:sort(counter_gen:merged(Plus1, Plus2)))},
                             {minus_eq, equals(lists:sort(Minus12),
                                               lists:sort(counter_gen:merged(Minus1, Minus2)))},
                             {self_eq, riak_crdt_pncounter:equal(riak_crdt_pncounter:merge(Counter1, Counter2), 
                                                                 riak_crdt_pncounter:merge(Counter2, Counter1))}])
            end
           ).

prop_equal() ->
    ?FORALL({{Plus1, Minus1}=Counter1, {Plus2, Minus2}=Counter2}, {counter_gen:pncounter(), counter_gen:pncounter()},
            begin
                Eq = riak_crdt_pncounter:equal(Counter1, Counter2),
                RevEq = riak_crdt_pncounter:equal(Counter2, Counter1),
                SelfEq = riak_crdt_pncounter:equal(Counter1, {lists:reverse(Plus1), lists:reverse(Minus1)}),

                OK = case lists:sort(Plus1) == lists:sort(Plus2) andalso lists:sort(Minus1) == lists:sort(Minus2) of
                         true ->
                             Eq andalso RevEq;
                         false ->
                             (not Eq) andalso not (RevEq)
                     end,
                conjunction([{eq, OK}, {self_eq, SelfEq}])
            end
           ).
                       

%% helpers
total({Plus, Minus}=Counter) when is_tuple(Counter) ->
    total(Plus) - total(Minus);
total(Counter) ->
    lists:sum([I || {_, I} <- Counter]).
