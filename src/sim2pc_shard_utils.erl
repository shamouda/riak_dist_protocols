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

-module(sim2pc_shard_utils).

%% API
-export([
    send_to_one_and_return_node/3,
    send_to_one/2
]).

send_to_one_and_return_node(Bucket, Key, Cmd) ->
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, simple_kv_key_store),
    [{IndexNode, _Type}] = PrefList,
    Response = riak_core_vnode_master:sync_spawn_command(IndexNode, Cmd, sim2pc_cohort_vnode_master),
    {IndexNode, Response}.

send_to_one(IndexNode, Cmd) ->
    riak_core_vnode_master:sync_spawn_command(IndexNode, Cmd, sim2pc_cohort_vnode_master).

