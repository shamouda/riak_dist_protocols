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

-module(gpac_leader).

-behavior(gen_statem).

-include_lib("kernel/include/logger.hrl").


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
	used_shards,  %% which shards used within the transaction
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
    {ok, ready_to_start, #state{}}.

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

waiting_for_op({call, Sender}, {tx_put, Bucket, Key, Value}, State = #state{tx_id = TxId}) ->
	?LOG_INFO("current_state[waiting_for_op] action[tx_put, ~p, ~p]", [Key, Value]),
    {IndexNode, _} = sim2pc_shard_utils:send_to_one_and_return_node(Bucket, Key, {tx_put, TxId, {Key, Value}}),
    {next_state, waiting_for_op, State, {reply, Sender, {ok, IndexNode}}};

waiting_for_op({call, Sender}, {tx_get, Bucket, Key}, State = #state{tx_id = TxId}) ->
	?LOG_INFO("current_state[waiting_for_op] action[tx_get, ~p]", [Key]),
    {IndexNode, Response} = sim2pc_shard_utils:send_to_one_and_return_node(Bucket, Key, {tx_get, TxId, {Key}}),
    {_, {value, Value}, _} = Response,
    {next_state, waiting_for_op, State, {reply, Sender, {ok, IndexNode, Value}}};

waiting_for_op({call, Sender}, {prepare, NodesWithDuplicates}, State = #state{tx_id = TxId}) ->
	?LOG_INFO("current_state[waiting_for_op] action[prepare]", []),
    Nodes = lists:usort(NodesWithDuplicates), % to remove duplicates
    Response = lists:foldl(fun(Node, OutList) -> 
                    Result = sim2pc_shard_utils:send_to_one(Node, {prepare, TxId}),
                    [Result | OutList]
                end, [], Nodes),
    ?LOG_INFO("current_state[waiting_for_op] action[prepare] Response = ~p", [Response]),
    ToAbort = lists:member(abort, Response),
    case ToAbort of
        false ->
            {next_state, waiting_to_commit, State#state{status = waiting_to_commit}, {reply, Sender, ok}};
        true ->
            {next_state, waiting_to_abort, State#state{status = waiting_to_abort}, {reply, Sender, abort}}
    end.

waiting_to_commit({call, Sender}, {commit, NodesWithDuplicates}, State = #state{tx_id = TxId}) ->
	?LOG_INFO("current_state[waiting_to_commit] action[commit]", []),
    Nodes = lists:usort(NodesWithDuplicates), % to remove duplicates
    lists:foreach(fun(Node) -> sim2pc_shard_utils:send_to_one(Node, {commit, TxId}) end, Nodes),
    {next_state, committed, State#state{status = committed}, {reply, Sender, ok}}.

waiting_to_abort({call, Sender}, {abort, NodesWithDuplicates}, State = #state{tx_id = TxId}) ->
	?LOG_INFO("current_state[waiting_to_abort] action[abort]", []),
    Nodes = lists:usort(NodesWithDuplicates), % to remove duplicates
    lists:foreach(fun(Node) -> sim2pc_shard_utils:send_to_one(Node, {abort, TxId}) end, Nodes),
    {next_state, aborted, State#state{status = aborted}, {reply, Sender, ok}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
