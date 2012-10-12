%% -------------------------------------------------------------------
%%
%% riak_dt_wm_pncounter: Webmachine resource for PN-Counter
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_dt_wm_pncounter).
-compile([export_all]).

-include_lib("webmachine/include/webmachine.hrl").

-record(state, {key :: string(),
                value :: term(),
                action :: {increment | decrement, integer()},
                timeout :: pos_integer() | infinity | undefined}).

add_routes() ->
    webmachine_router:add_route({["counters", key], ?MODULE, []}).

init(_Props) ->
    {ok, #state{}}.

malformed_request(RD, State) ->
    case wrq:method(RD) of
        'POST' ->
            case catch list_to_integer(binary_to_list(wrq:req_body(RD))) of
                {'EXIT', _} ->
                    {true, RD, State};
                Change when Change < 0 ->
                    {false, RD, State#state{action={decrement, -Change}}};
                Change ->
                    {false, RD, State#state{action={increment, Change}}}
            end;
        _ ->
            {false, RD, State}
    end.

resource_exists(RD, State) ->
    Key = list_to_binary(wrq:path_info(key, RD)),
    Timeout = wrq:get_qs_value("timeout", RD),
    case wrq:method(RD) of
        'POST' ->
            {true, RD, State#state{key=Key, timeout=Timeout}};
        _ ->
            case get_value(Key, Timeout) of
                Count when is_integer(Count) ->
                    {true, RD, State#state{value=Count, key=Key}};
                notfound ->
                    {false, RD, State#state{key=Key}};
                {error, Reason} ->
                    {{halt, 500}, wrq:set_resp_body(err_msg(Reason), RD), State#state{key=Key}}
            end
    end.

get_value(Key, undefined) ->
    riak_dt_client:value(riak_dt_pncounter, Key);
get_value(Key, "infinity") ->
    riak_dt_client:value(riak_dt_pncounter, Key, infinity);
get_value(Key, Timeout) ->
    riak_dt_client:value(riak_dt_pncounter, Key, list_to_integer(Timeout)).

allowed_methods(RD, State) ->
    {['GET', 'HEAD', 'POST'], RD, State}.

err_msg(Err) when is_atom(Err) ->
    atom_to_binary(Err, utf8).

content_types_provided(RD, State) ->
    {[{"text/plain", to_text}], RD, State}.

process_post(RD, #state{key=Key, action=UpdateOp, timeout=Timeout}=State) ->
    case do_update(Key, UpdateOp, Timeout) of
        ok ->
            {true, RD, State};
        {error, timeout} ->
            {{halt, 503}, wrq:set_resp_body("Request timed out", RD), State};
        {error, Reason} ->
            {{halt, 500}, wrq:set_resp_body(Reason, RD), State};
        _Other ->
            {false, RD, State}
    end.

do_update(Key, UpdateOp, undefined) ->
    riak_dt_client:update(riak_dt_pncounter, Key, UpdateOp);
do_update(Key, UpdateOp, "infinity") ->
    riak_dt_client:update(riak_dt_pncounter, Key, UpdateOp, infinity);
do_update(Key, UpdateOp, Timeout) ->
    riak_dt_client:update(riak_dt_pncounter, Key, UpdateOp, list_to_integer(Timeout)).

to_text(RD, #state{value=Value}=State) ->
    {integer_to_list(Value), RD, State}.
