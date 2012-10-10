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

-module(riak_dt_wm_demo).
-compile([export_all]).

-include_lib("webmachine/include/webmachine.hrl").

init(_Props) ->
    {ok, []}.

add_routes() ->
    webmachine_router:add_route({["ricon", file], ?MODULE, []}).

allowed_methods(RD, Ctx) ->
    {['GET'], RD, Ctx}.

content_types_provided(RD, Ctx) ->
    {[{"text/html", serve_file}, {"text/javascript", serve_file}], RD, Ctx}.

serve_file(RD, Ctx) ->
    File = proplists:get_value(file, dict:to_list(wrq:path_info(RD)), "index.html"),
    lager:debug("F ~p", [File]),
    {ok, Bin} = file:read_file(filename:join([code:priv_dir(riak_dt), "www", File])),
    {Bin, RD, Ctx}.
