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

-module(gpac_shard_utils).

%% API
-export([
    send_to_quorum_and_get_nodes/5,
    send_to_quorum/3,
    send_to_quorum_old/6
]).

send_to_quorum_and_get_nodes(Bucket, Key, Cmd, ReplicaCount, TimeOut) -> 
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, ReplicaCount, simple_kv_key_store),
    Responses = pmap(fun({IndexNode, _Type}) -> 
                    riak_core_vnode_master:sync_command(IndexNode, Cmd, gpac_cohort_vnode_master, TimeOut) 
                end, PrefList),
    {PrefList, Responses}.

send_to_quorum(Nodes, Cmd, TimeOut) ->
    Responses = pmap(fun({IndexNode, _Type}) -> 
                    riak_core_vnode_master:sync_command(IndexNode, Cmd, gpac_cohort_vnode_master, TimeOut) 
                end, Nodes),
    {Nodes, Responses}.
 
 send_to_quorum_old(Bucket, Key, Cmd, ReplicaCount, QuorumSize, TimeOut) -> 
    DocIdx = riak_core_util:chash_key({Bucket, Key}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, ReplicaCount, simple_kv_key_store),
    Responses = pmap(fun({IndexNode, _Type}) -> 
                    riak_core_vnode_master:sync_command(IndexNode, Cmd, gpac_cohort_vnode_master, TimeOut) 
                end, PrefList),

    {ResponseCount, Results} = lists:foldl(fun(Response, {Count, TmpResults}) ->
            case Response of
                {{request_id, _ReqId}, _Result} -> {Count - 1, [TmpResults | Response]} ;
                _ -> {Count, [TmpResults | Response]}
            end
        end, {QuorumSize, []}, Responses),

    case ResponseCount =< 0 of
        true -> {PrefList, Results};
        _ -> error
    end.
%% -------------
%% Utility function
%% -------------
pmap(F, L) ->
     Parent = self(),
     lists:foldl(
         fun(X, N) ->
             spawn_link(fun() ->
                            Parent ! {pmap, N, F(X)}
                        end),
             N+1
         end, 0, L),
     L2 = [receive {pmap, N, R} -> {N, R} end || _ <- L],
     {_, L3} = lists:unzip(lists:keysort(1, L2)),
     L3.