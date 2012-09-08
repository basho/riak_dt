%% -------------------------------------------------------------------
%%
%% riak_dt_wm_orset: Webmachine resource for OR-Set access
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

-module(riak_dt_wm_orset).
-compile([export_all]).

-include_lib("webmachine/include/webmachine.hrl").

-record(state, {key :: string(),
                action :: atom(),
                value :: term()}).
add_routes() ->
    webmachine_router:add_route({["sets", key], ?MODULE, [{action, value}]}),
    webmachine_router:add_route({["sets", key, "add"], ?MODULE, [{action, add}]}),
    webmachine_router:add_route({["sets", key, "remove"], ?MODULE, [{action, remove}]}).

init(Props) ->
    {ok, #state{
       action=proplists:get_value(action, Props)
      }}.

resource_exists(RD, #state{action=Action}=State) ->
    Key = wrq:path_info(key, RD),
    case Action of
        value ->
            case riak_dt_client:value(riak_dt_orset, Key) of
                Val when is_list(Val) ->
                    {true, RD, State#state{value=Val, key=Key}};
                notfound ->
                    {false, RD, State#state{key=Key}};
                Other ->
                    lager:info("Got error when fetching set ~w: ~w~n",[Key, Other]),
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
    UpdateOp = {Action, wrq:req_body(RD)},
    case riak_dt_client:update(riak_dt_orset, Key, UpdateOp) of
        ok ->
            {true, RD, State};
        {error, timeout} ->
            {{halt, 503}, wrq:set_resp_body("Request timed out", RD), State};
        {error, Reason} ->
            {{halt, 500}, wrq:set_resp_body(Reason, RD), State};
        _Other ->
            {false, RD, State}
    end.

to_text(RD, #state{key=Key, value=Value}=State) ->
    JSON = mochijson2:encode({struct, [{Key, Value}]}),
    {JSON, RD, State}.
