-module(update_fsm_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

update_primary_only_test_() ->
    {setup,
     fun() ->
             riak_dt_test_util:setup("update_primary_only")
     end,
     fun riak_dt_test_util:cleanup/1,
     ?_test(begin
                meck:new(riak_core_apl, [passthrough]),
                meck:expect(riak_core_apl, get_apl_ann,
                            fun(_DocIdx, _N, _Ring, _UpNodes) ->
                                    %% Create a fake preflist that only includes fallbacks
                                    [{{0, node()}, fallback},
                                     {{1, node()}, fallback},
                                     {{2, node()}, fallback}]
                            end),
                ?assertEqual({error, no_primaries}, riak_dt_client:update(riak_dt_pncounter, <<"foo">>, {increment, 5})),
                meck:unload()
            end)
    }.
