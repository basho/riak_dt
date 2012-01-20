-module(gcounter_eqc).
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
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_increment()))).

%% eqc_increment_by_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_increment_by()))).

%% eqc_merge_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_merge()))).

%% eqc_equal_test() ->
%%     eqc:quickcheck(eqc:numtests(?NUMTESTS, ?QC_OUT(prop_equal()))).

prop_value() ->
    ?FORALL(Counter, counter_gen:gcounter(),
            equals(riak_crdt_gcounter:value(Counter), lists:sum([I || {_, I} <- Counter]))).

prop_increment() ->
    ?FORALL({Counter, Actor}, {counter_gen:gcounter(), int()},
            begin
                Counter2 =  riak_crdt_gcounter:update(increment, Actor, Counter),
                conjunction([
                             {actor_count, equals(counter_gen:actor_count(Actor, Counter2),
                                                  (counter_gen:actor_count(Actor, Counter) +1))},
                             {total, equals(riak_crdt_gcounter:value(Counter2),
                                            (lists:sum([I || {_, I} <- Counter]) +1))
                             }
                            ])
            end
           ).

prop_increment_by() ->
    ?FORALL({Counter, Actor, By}, {counter_gen:gcounter(), int(), int()},
            begin
                case catch( riak_crdt_gcounter:update({increment, By}, Actor, Counter) )  of
                    {'EXIT',{function_clause,[{riak_crdt_gcounter,update,[{increment, By}, Actor, Counter]}| _]}} ->
                        By < 1;
                    Counter2 ->
                        conjunction([
                                     {actor_count, equals(counter_gen:actor_count(Actor, Counter2),
                                                          (counter_gen:actor_count(Actor, Counter) + By))},
                                     {total, equals(riak_crdt_gcounter:value(Counter2),
                                                    (lists:sum([I || {_, I} <- Counter]) + By))}
                                    ])
                end
            end
           ).

prop_merge() ->
    ?FORALL({Counter1, Counter2}, {counter_gen:gcounter(), counter_gen:gcounter()},
            conjunction([
                         {eq, equals(lists:sort(riak_crdt_gcounter:merge(Counter1, Counter2)),
                                lists:sort(counter_gen:merged(Counter1, Counter2)))},
                         {self_eq, equals(lists:sort(riak_crdt_gcounter:merge(Counter1, Counter2)),
                                lists:sort(riak_crdt_gcounter:merge(Counter2, Counter1)))}
                        ])
           ).

prop_equal() ->
    ?FORALL({Counter1, Counter2}, {counter_gen:gcounter(), counter_gen:gcounter()},
            begin
                Eq =  riak_crdt_gcounter:equal(Counter1, Counter2),
                Eq2 =  riak_crdt_gcounter:equal(Counter2, Counter1),
                Eq3 = riak_crdt_gcounter:equal(Counter1, lists:reverse(Counter1)),
                OK = case lists:sort(Counter1) == lists:sort(Counter2) of
                         true ->
                             Eq andalso Eq2;
                         false ->
                             (not Eq) andalso (not Eq2)
                     end,
                conjunction([{eq_neq, OK}, {self_eq, Eq3}])
            end).
