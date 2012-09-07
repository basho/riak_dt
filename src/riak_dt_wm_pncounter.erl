-module(riak_dt_wm_pncounter).
-compile([export_all]).

-include_lib("webmachine/include/webmachine.hrl").

-record(state, {key :: string(),
                action :: atom(),
                value :: term()}).
add_routes() ->
    webmachine_router:add_route({["counters", key], ?MODULE, [{action, value}]}),
    webmachine_router:add_route({["counters", key, "increment"], ?MODULE, [{action, increment}]}),
    webmachine_router:add_route({["counters", key, "decrement"], ?MODULE, [{action, decrement}]}).

init(Props) ->
    {ok, #state{
       action=proplists:get_value(action, Props)
      }}.

resource_exists(RD, #state{action=Action}=State) ->
    Key = wrq:path_info(key, RD),
    case Action of
        value ->
            case riak_dt_client:value(riak_dt_pncounter, Key) of
                Count when is_integer(Count) ->
                    {true, RD, State#state{value=Count, key=Key}};
                notfound ->
                    {false, RD, State#state{key=Key}};
                Other ->
                    lager:info("Got error when fetching counter ~w: ~w~n",[Key, Other]),
                    {false, RD, State#state{key=Key}}
            end;
        _ ->
            {true, RD, State#state{key=Key}}
    end.

allowed_methods(RD, #state{action=Action} = State) ->
    case Action of
        value ->
            {['GET', 'HEAD'], RD, State};
        _ ->
            {['POST'], RD, State}
    end.

content_types_provided(RD, State) ->
    {[{"text/plain", to_text}], RD, State}.

process_post(RD, #state{action=Action, key=Key}=State) ->
    UpdateOp = get_action(Action, wrq:req_body(RD)),
    case riak_dt_client:update(riak_dt_pncounter, Key, UpdateOp) of
        ok ->
            {true, RD, State};
        {error, timeout} ->
            {{halt, 503}, wrq:set_resp_body("Request timed out", RD), State};
        {error, Reason} ->
            {{halt, 500}, wrq:set_resp_body(Reason, RD), State};
        _Other ->
            {false, RD, State}
    end.

to_text(RD, #state{value=Value}=State) ->
    {integer_to_list(Value), RD, State}.

get_action(Action, Val0)  ->
    Val = binary_to_list(Val0),
    case (catch list_to_integer(Val)) of
        I when is_integer(I), I > 0 ->
            {Action, I};
        _ ->
            Action
    end.
