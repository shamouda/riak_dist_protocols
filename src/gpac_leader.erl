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

%% TODO: leader must record its last sequence number

-module(gpac_leader).

-behavior(gen_statem).

-include_lib("kernel/include/logger.hrl").

%% start quorum constants
-define(REPLICAS, 3).
-define(QUORUM_SIZE, 2).
-define(TIMEOUT, 1000). %% in millisecond
%% end quorum constants


%% API
-export([
    start_link/0
]).

%% gen_statem callbacks
-export([
    callback_mode/0,
    init/1,
    format_status/2,
    terminate/3,
    code_change/4
]).

%% states
-export([
    ready_to_start/3,
    waiting_for_op/3,
    waiting_to_commit/3,
    waiting_to_abort/3
]).

-record(state, {
	tx_id,
	used_shards,  %% map from master shard -> replicas of master shard
	status        %% transaction state : active | prepared | committing |
   				   %  committed | committed_read_only |
    			   %  undefined | aborted
    			   %%
    
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_statem:start_link(?MODULE, [], []).
    
%%% TODO: why this does not work?
%%% gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================
callback_mode() ->
    state_functions.

init([]) ->
	?LOG_INFO("State machine initialized next_state[ready_to_start]", []),
    {ok, ready_to_start, #state{ used_shards = maps:new() }}.

format_status(_Opt, [_PDict, State, Data]) ->
    [{data, [{"State", {State, Data}}]}].

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================
%%% state transitions
%%%===================
ready_to_start({call, Sender}, {start_tx, TxId}, State) ->
	?LOG_INFO("current_state[ready_to_start] action[start_tx, ~p]", [TxId]),
    {next_state, waiting_for_op, State#state{tx_id = TxId, status = active}, {reply, Sender, ok}}.

waiting_for_op({call, Sender}, {tx_put, Bucket, Key, Value}, State = #state{tx_id = TxId, used_shards = OldShards}) ->
	?LOG_INFO("current_state[waiting_for_op] action[tx_put, ~p, ~p]", [Key, Value]),
    {PrefList, Responses} = gpac_shard_utils:send_to_quorum_and_get_nodes(Bucket, Key, {tx_put, TxId, {Key, Value}}, ?REPLICAS, ?TIMEOUT),
    Success = validate_put_responses(Responses, ?REPLICAS),
    case Success of
        false ->
            {next_state, waiting_to_abort, State#state{status = waiting_to_abort}, {reply, Sender, abort}};
        true ->
            IndexNodes = [ IndexNode || {IndexNode, _Type} <- PrefList],
            [Master | Others] = IndexNodes,
            NewShards = case maps:is_key(Master, OldShards) of
                            true -> OldShards;
                            false -> maps:put(Master, Others, OldShards)
                        end,
            {next_state, waiting_for_op, State#state{used_shards= NewShards}, {reply, Sender, {ok, IndexNodes}}}
    end;
  
waiting_for_op({call, Sender}, {tx_get, Bucket, Key}, State = #state{tx_id = TxId, used_shards = OldShards}) ->
	?LOG_INFO("current_state[waiting_for_op] action[tx_get, ~p]", [Key]),
    {PrefList, Responses} = gpac_shard_utils:send_to_quorum_and_get_nodes(Bucket, Key, {tx_get, TxId, {Key}}, ?REPLICAS, ?TIMEOUT),
    {Success, Value} = validate_get_responses(Responses, ?REPLICAS),
    case Success of
        false ->
            {next_state, waiting_to_abort, State#state{status = waiting_to_abort}, {reply, Sender, abort}};
        true ->
            IndexNodes = [ IndexNode || {IndexNode, _Type} <- PrefList],
            [Master | Others] = IndexNodes,
            NewShards = case maps:is_key(Master, OldShards) of
                            true -> OldShards;
                            false -> maps:put(Master, Others, OldShards)
                        end,
            {next_state, waiting_for_op, State#state{used_shards= NewShards}, {reply, Sender, {ok, IndexNodes, Value}}}
    end;

waiting_for_op({call, Sender}, {elect_and_prepare}, State = #state{tx_id = TxId, used_shards = Shards}) ->
	?LOG_INFO("current_state[waiting_for_op] action[prepare] shards[~p]", [Shards]),
    MapEntries = maps:to_list(Shards),
    Nodes = lists:foldl( fun({Master, Replicas}, InList) -> 
        [InList | [Master | Replicas]]
        end, [], MapEntries),
    ?LOG_INFO("===>current_state[waiting_for_op] Nodes[~p]", [Nodes]),

    Responses = lists:foldl(fun({Master, Replicas}, OutList) ->
            Nodes = [Master | Replicas], 
            {_RepliedShards, Responses} = gpac_shard_utils:send_to_quorum(Nodes, {elect_and_prepare, TxId, Master}, ?TIMEOUT),
            [Responses | OutList]
        end, [], MapEntries),

    Success = validate_elect_and_prepare_responses(Responses, Shards),
    case Success of
        true -> 
            {next_state, waiting_to_commit, State#state{status = waiting_to_commit}, {reply, Sender, ok}};
        false -> 
            {next_state, waiting_to_abort, State#state{status = waiting_to_abort}, {reply, Sender, abort}}
    end.

waiting_to_commit({call, Sender}, {commit}, State = #state{tx_id = TxId, used_shards = Shards}) ->
	?LOG_INFO("current_state[waiting_to_commit] action[commit] shards[~p]", [Shards]),
    Nodes = sets:to_list(Shards),
    lists:foreach(fun(Node) -> sim2pc_shard_utils:send_to_one(Node, {commit, TxId}) end, Nodes),
    {next_state, committed, State#state{status = committed}, {reply, Sender, ok}}.

waiting_to_abort({call, Sender}, {abort}, State = #state{tx_id = TxId, used_shards = Shards}) ->
	?LOG_INFO("current_state[waiting_to_abort] action[abort] shards[~p]", [Shards]),
    Nodes = sets:to_list(Shards),
    lists:foreach(fun(Node) -> sim2pc_shard_utils:send_to_one(Node, {abort, TxId}) end, Nodes),
    {next_state, aborted, State#state{status = aborted}, {reply, Sender, ok}}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

validate_put_responses([], _) -> false;
validate_put_responses(Responses, QuorumSize) ->
    ResponseCount = lists:foldl(fun(Response, Count) ->
        case Response of
                {{request_id, _ReqId}, _Result} -> Count - 1 ;
                _ -> Count
            end
        end, QuorumSize, Responses),
    Success = ResponseCount =< 0,
    ?LOG_INFO("===>validate_put_responses Responses[~p] Success[~p]", [Responses, Success]),
    Success.

validate_get_responses([], _) -> {false, undefined};
validate_get_responses(Responses, QuorumSize) ->
    [ TmpResponse | _ ] = Responses,
    {_, {value, ExpectedValue}, _} = TmpResponse,
    ResponseCount = lists:foldl(fun(Response, Count) ->
        case Response of
                {_, {value, ExpectedValue} , _} -> Count - 1 ;
                _ -> Count
            end
        end, QuorumSize, Responses),
    Success = ResponseCount =< 0,
    ?LOG_INFO("===>validate_get_responses Responses[~p] ExpectedValue[~p] Success[~p]", [Responses, ExpectedValue, Success]),
    {Success, ExpectedValue}.

%% we need a superset response and no single abort
validate_elect_and_prepare_responses(Responses, Shards) ->
    MasterNodes = maps:keys(Shards),
    TmpCountMap = lists:foldl(fun(Master, TmpMap) ->
            maps:put(Master, 0, TmpMap)
        end, maps:new(), MasterNodes),
    CountMap = lists:foldl(fun(Response, TmpMap) ->
        case Response of
                {_ , {location, _IndexNode}, {master, Master}} -> 
                    maps:put(Master, maps:get(Master, TmpMap) + 1, TmpMap);
                _ -> TmpMap
            end
        end, TmpCountMap, Responses),
    CountList = maps:to_list(CountMap),
    ToAbort = lists:member(abort, Responses),
    Result = lists:foldl(fun({_Master, Count}, Success) ->
            Success and Count > ?REPLICAS / 2
        end, ToAbort, CountList),
    Result.