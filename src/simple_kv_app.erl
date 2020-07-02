%% -------------------------------------------------------------------
%%
%% Copyright <2013-2020> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
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
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

-module(simple_kv_app).

-behaviour(application).

-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    ok = validate_data_dir(),

    case simple_kv_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, sim2pc_cohort_vnode}]),
            ok = riak_core_node_watcher:service_up(simple_kv_key_store, self()),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

validate_data_dir() ->
    DataDir = "data",
    case filelib:ensure_dir(filename:join(DataDir, "dummy")) of
        ok -> ok;
        {error, Reason} ->
            logger:critical("Data directory ~p does not exist, and could not be created: ~p", [DataDir, Reason]),
            throw({error, invalid_data_dir})
    end.

stop(_State) -> ok.
