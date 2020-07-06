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

-module(simple_kv).

%% start quorum constants
-define(QUORUM_SIZE, 3).
-define(W, 2).
-define(R, 1).
-define(TIMEOUT, 1000). %% in millisecond
%% end quorum constants

%% API
-export([
    get/2,
    put/3,
    keys/0,
    qget/2,
    qput/3
]).

get(Bucket, Key) ->
    ReqId = make_ref(),
    sim2pc_shard_utils:send_to_one_and_return_node(Bucket, Key, {get, ReqId, {Key}}).

put(Bucket, Key, Value) ->
    ReqId = make_ref(),
    sim2pc_shard_utils:send_to_one_and_return_node(Bucket, Key, {put, ReqId, {Key, Value}}).

keys() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Indices = riak_core_ring:my_indices(Ring),
    logger:info("length of indices = ~p", [length(Indices)]),
    lists:foldl(fun(Index, Responses) ->
        {keys, KeysOfIndex} = riak_core_vnode_master:sync_spawn_command({Index, node()}, {list_keys}, sim2pc_cohort_vnode_master),
        Responses ++ KeysOfIndex
                end, [], Indices).

qget(Bucket, Key) ->
    ReqId = make_ref(),
    sim2pc_shard_utils:send_to_quorum(Bucket, Key, {get, ReqId, {Key}}, ?R, ?TIMEOUT).

qput(Bucket, Key, Value) ->
    ReqId = make_ref(),
    sim2pc_shard_utils:send_to_quorum(Bucket, Key, {put, ReqId, {Key, Value}}, ?W, ?TIMEOUT).
