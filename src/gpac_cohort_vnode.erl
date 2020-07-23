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

-module(gpac_cohort_vnode).

-behaviour(riak_core_vnode).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
    start_vnode/1,
    init/1,
    handle_command/3,
    terminate/2,
    is_empty/1,
    delete/1,
    handle_handoff_command/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_data/2,
    handle_overload_command/3,
    handle_overload_info/2,
    handle_coverage/4,
    handle_exit/3,
    encode_handoff_item/2
]).

-ignore_xref([start_vnode/1]).

%% TODO: gpac_cohort_vnode must record the latest accepted leader
%% TODO: the state must be reset after each transaction (can we use a statem here?)
%% TODO: remove the constants REPLICAS from here. Only set it in the leader module
%% TODO: pac_elect_me must respond by pack_elect_you command. This will break down the committment to multiple phases and avoid blocking
%% TODO: pac_apply must be sent asynchronously by leader

-define(REPLICAS, 3).

%assuming we handle just one request at a time
-record(state, {
    partition, 
    kv_state,  %stable k/v values (committed values)
    kv_pending, %non-committed changes  ReqId -> K/V Map 
    prepared,  %prepared transactions
    
    %%% state variables for transaction commit %%%
    ballot_num,
    init_val,    % commit | abort | undefined
    accept_num,
    accept_val,
    decision,

    promissed_ballot_num
}).

-spec start_vnode(integer()) -> any().
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%who will give the partition parameter
init([Partition]) ->
    {ok, #state {  partition = Partition, 
                   kv_state = maps:new(), 
                   kv_pending = maps:new(), 
                   prepared = sets:new(),
                   
                   ballot_num = {0, Partition},
                   init_val = undefined,
                   accept_num = {0, Partition},
                   accept_val = null,
                   decision = false,

                   promissed_ballot_num = {-1, -1}
                    }}.

handle_command({get, ReqId, {Key}}, _Sender, State = #state{kv_state = KvState}) ->
    Value = maps:get(Key, KvState, not_assigned),
    {reply, {{request_id, ReqId}, {result, Value}}, State};

handle_command({put, ReqId, {Key, Value}}, _Sender, State = #state{kv_state = KvState, partition = Partition}) ->
    Location = [Partition, node()],
    NewKvState = maps:put(Key, Value, KvState),
    {reply, {{request_id, ReqId}, {location, Location}}, State#state{kv_state = NewKvState}};

handle_command({list_keys}, _Sender, State = #state{kv_state = KvState, partition = _Partition}) ->
    {reply, {keys, maps:keys(KvState)}, State};

handle_command({tx_put, ReqId, {Key, Value}}, _Sender, State = #state{kv_pending = PendingState, partition = Partition, prepared = Prepared}) ->
    Location = [Partition, node()],
    logger:info("handle_command(tx_put) here=~p", [Location]),
    case sets:is_element(ReqId, Prepared) of
        true -> {reply, error, State};
        false -> append_pending_values(State, PendingState, ReqId, Key, Value, Partition)
    end;

handle_command({tx_get, ReqId, {Key}}, _Sender, State = #state{kv_pending = PendingState, kv_state = KvState, partition = Partition, prepared = Prepared}) ->
    Location = [Partition, node()],
    logger:info("handle_command(tx_get) here=~p", [Location]),
    case sets:is_element(ReqId, Prepared) of
        true -> {reply, error, State};
        false -> read_value(State, PendingState, KvState, ReqId, Key, Partition)
    end;

%% Here the partition tries to become the leader. 
%% If successful, it proceeds into replicating the transaction's decision.
handle_command({end_transaction, ReqId, Shards, Timeout}, _Sender, State = #state{prepared = Prepared, partition = Partition, 
    ballot_num = {Num, P}, 
    init_val = InitValue, 
    accept_num = AcceptNum, 
    accept_val = AcceptValue, 
    decision = Decision}) ->

    Location = [Partition, node()],
    logger:info("handle_command(end_transaction) here=~p", [Location]),
    NewBallot = {Num + 1, P},
    NewInitValue = case conflict_exist() of
        true -> abort;
        false -> commit
    end,

    MapEntries = maps:to_list(Shards),
    ElectionResponse = lists:foldl(fun({Master, Replicas}, InMap) ->
            Nodes = [Master | Replicas], 
            {_RepliedShards, Responses} = gpac_shard_utils:send_to_quorum(Nodes, {pac_elect_me, ReqId, NewBallot}, Timeout),
            maps:put(Master, Responses, InMap)
        end, #{}, MapEntries),
    
    %% flatten the responses to make the computations easier
    TmpResponses = [ Responses || {_Master, Responses} <- ElectionResponse],
    FlatResponses = lists:flatten(TmpResponses),

    ResponsesPerShard = count_valid_responses_per_shard(ElectionResponse),
    N = lists:size(MapEntries),
    R = ?REPLICAS,

    SuperMajority = is_super_majority(ResponsesPerShard, N, R),
    SuperSet = is_super_set(ResponsesPerShard, N, R),
    OneDecisionTrue = find_any_decision_true(ResponsesPerShard),
    OneAcceptedCommit = find_any_accepted_commit(ResponsesPerShard),
    SuperSetVote = case SuperSet of
        true -> get_super_set_vote(FlatResponses);
        false -> undefined
    end,
    AcceptValueFound = case OneDecisionTrue of
        true -> get_accepted_value_with_decision_true(FlatResponses);
        false -> undefined
    end,

    ElectionResult = process_election_response({SuperMajority, SuperSet, OneDecisionTrue, OneAcceptedCommit}, AcceptValueFound, SuperSetVote),

    %%TODO: handle the different cases
    NewAcceptVal = AcceptValueFound,
    NewAcceptNum = NewBallot,
    AgreementResponse = lists:foldl(fun({Master, Replicas}, InMap) ->
            Nodes = [Master | Replicas], 
            {_RepliedShards, Responses} = gpac_shard_utils:send_to_quorum(Nodes, {pac_ft_agree, ReqId, NewAcceptVal, NewAcceptNum}, Timeout),
            maps:put(Master, Responses, InMap)
        end, #{}, MapEntries),

    ResponsesPerShard2 = count_valid_responses_per_shard(AgreementResponse),
    SuperMajority2 = is_super_majority(ResponsesPerShard2, N, R),

    NewDecision = true, %%TODO: only when we hear back from a majority
    %%TODO: no need to wait for responses
    ElectionResponse = lists:foldl(fun({Master, Replicas}, InMap) ->
        Nodes = [Master | Replicas], 
        {_RepliedShards, Responses} = gpac_shard_utils:send_to_quorum(Nodes, {pac_apply, ReqId, NewDecision, NewAcceptVal}, Timeout),
        maps:put(Master, Responses, InMap)
    end, #{}, MapEntries),

    {reply, {{request_id, ReqId}, {location, Location}}, State = #state{ballot_num = NewBallot, 
                                                                        init_val = NewInitValue,
                                                                        accept_val = NewAcceptVal,
                                                                        accept_num = NewAcceptNum,
                                                                        decision = NewDecision
                                                                        } };

%%process_election_response(Status = {_SuperMajority, _SuperSet, _OneDecisionTrue, _OneAcceptedCommit}, AcceptValueFound, SuperSetVote) ->
%%    case Status of 
%%        {true, _, true, _} -> {{branch, 1}, {decision, true}, {accept_val, AcceptValueFound}};
%%        {true, _, false, true} -> {{branch, 2}, {decision, true}, {accept_val, commit}}; %%TODO: is decision true here???
%%        {true, true, _, _} -> {{branch, 3}, {accept_val, SuperSetVote}}; %%TODO: is decision true here???
%%        {true, false, _, _} -> {{branch, 3}, {accept_val, abort}}; %%TODO: is decision true here???
%%       _ -> error
%%    end.

%% TODO: should better respond by invoking the pac_elect_you command to the leader, but this requires detecting timeouts by the leader
handle_command({pac_elect_me, ReqId, ProposedBallot}, _Sender, State = #state{partition = Partition,
    promissed_ballot_num = PromissedBallot,
    accept_num = AcceptNum, 
    accept_val = AcceptValue,
    decision = Decision}) ->
    Location = [Partition, node()],
    logger:info("handle_command(pac_elect_me) here=~p", [Location]),
    {N1, P1} = ProposedBallot,
    {N2, P2} = PromissedBallot,
    AcceptCondition = is_greater_ballot(ProposedBallot, PromissedBallot), %%true, %%(P1 == P2 and N2 > N1) or (P2 > P1),

    NextInitValue = case conflict_exist() of
        true -> abort;
        false -> commit
    end,

    case AcceptCondition of
        true -> {reply, {{request_id, ReqId}, 
                         {location, Location}, 
                         {init_val, NextInitValue}, 
                         {accept_val, AcceptValue}, 
                         {accept_num, AcceptNum}, 
                         {decision, Decision}}, 
                    State = #state{promissed_ballot_num = ProposedBallot, init_val = NextInitValue}};
        false -> {noreply, State}
    end;

%% TODO: should better response by invoking command pack-ft-agreed
handle_command({pac_ft_agree, ReqId, AcceptVal, AcceptNum}, _Sender, State = #state{partition = Partition,
    promissed_ballot_num = PromissedBallot}) ->
    Location = [Partition, node()],
    logger:info("handle_command(pac_ft_agree) here=~p", [Location]),
    AcceptCondition =  is_greater_or_equal_ballot(AcceptNum, PromissedBallot),
    case AcceptCondition of
        true -> {reply, {{request_id, ReqId}, 
                         {location, Location}}, 
                    State = #state{accept_val = AcceptVal, accept_num = AcceptNum}};
        false -> {noreply, State}
    end;

handle_command({pac_apply, _ReqId, Decision, AcceptVal}, _Sender, State = #state{partition = Partition,
    promissed_ballot_num = PromissedBallot}) ->
    Location = [Partition, node()],
    logger:info("handle_command(pac_apply) here=~p", [Location]),
    case AcceptVal of
        commit -> perform_commit();
        abort -> perform_abort()
    end,
    {noreply, State = #state{decision = Decision, accept_val = AcceptVal}};

%% TODO: implementing separate command pac_elect_you leads to more efficient and reliable execution
%%handle_command({pac_elect_you, ReqId, Shards}, _Sender, State = #state{prepared = Prepared, partition = Partition}) ->
%%    Location = [Partition, node()],
%%    logger:info("handle_command(prepare) here=~p", [Location]),
%%    {reply, {{request_id, ReqId}, {location, Location}}, State};


handle_command(Message, _Sender, State) ->
    logger:warning("unhandled_command ~p", [Message]),
    {noreply, State}.

%%%%%%% Utility functions %%%%%%%  
perform_commit() -> ok.
perform_abort() -> ok.

%% count valid responses per shard
%% returns a list of size N
count_valid_responses_per_shard(ElectionResponse) ->
    CountFun = fun({_Master, Responses}) ->
        lists:foldl(fun(Response, Count) ->
                    case Response of
                        {{request_id, _ReqId}, _Result} -> Count + 1;
                        _ -> Count
                    end
                end, 0, Responses)
        end,
    lists:map(CountFun, ElectionResponse).

%% we must have a majority of replicas for EACH shard,
%% that is why the filtered list must be of size N.
%% N is the number of shards
%% R is the number of replicas of a shard
is_super_set(ResponsesPerShard, N, R) ->
    FilterPred = fun(I) -> I > R / 2 end,
    ShardsWithMajority = lists:filter(FilterPred, ResponsesPerShard),
    lists:length(ShardsWithMajority) == N.

%% we must have a majority of replicas for a majority of shards
%% that is why the filtered list must be of size N.
%% N is the number of shards
%% R is the number of replicas of a shard
is_super_majority(ResponsesPerShard, N, R) ->
    FilterPred = fun(I) -> I > R / 2 end,
    ShardsWithMajority = lists:filter(FilterPred, ResponsesPerShard),
    lists:length(ShardsWithMajority) > N / 2.

find_any_shard(ResponsePredicate, ElectionResponse) ->
    FilterPred = fun({_Master, Responses}) -> 
        Filtered = lists:filter(ResponsePredicate, Responses),
        lists:length(Filtered) > 0
    end,

    MatchingList = lists:filter(FilterPred, ElectionResponse),
    lists:length(MatchingList) > 0.

find_any_decision_true(ElectionResponse) ->
    DecisionTruePred = fun(Response) ->
        case Response of
            {{request_id, _ReqId}, {decision, true}, _Result} -> true;
            _ -> false
        end
    end,

    find_any_shard(DecisionTruePred, ElectionResponse).

find_any_accepted_commit(ElectionResponse) ->
    AcceptValCommitPred = fun(Response) ->
        case Response of
            {{request_id, _ReqId}, {accept_val, commit}, _Result} -> true;
            _ -> false
        end
    end,

    find_any_shard(AcceptValCommitPred, ElectionResponse).

%%precondition: call this only if the responses are a super-set
get_super_set_vote(FlatResponses) ->
    %% check if everyone in the super-set voted to commit
    AllCommit = lists:foldl(fun(Response, ValidFlag) ->
        IsCommit = case Response of
            {{request_id, _ReqId}, _Result1} -> 
                case Response of
                    {{request_id, _ReqId}, {init_val, commit}, _Result2} -> true;
                    {{request_id, _ReqId}, {init_val, abort}, _Result3} -> false; %% todo: do we need this condition??!!
                    _ -> false
                end;
            _ -> true %% ignore failed replicas in a super-set
        end,
        ValidFlag and IsCommit
    end, true, FlatResponses),

    case AllCommit of
        true -> commit;
        false -> abort
    end.

get_accepted_value_with_decision_true(FlatResponses) ->
    list:fold(fun(Response, PrevValue) ->
            case Response of 
                {{request_id, _ReqId}, {decision, true}, {accept_val, Val}, _Result} -> true;
                    case PrevValue of
                        undefined -> Val;
                        _ -> PrevValue
                    end;
                _ -> PrevValue
            end
        end, undefined, FlatResponses).

process_election_response(Status = {_SuperMajority, _SuperSet, _OneDecisionTrue, _OneAcceptedCommit}, AcceptValueFound, SuperSetVote) ->
    case Status of 
        {true, _, true, _} -> {{branch, 1}, {decision, true}, {accept_val, AcceptValueFound}};
        {true, _, false, true} -> {{branch, 2}, {decision, true}, {accept_val, commit}}; %%TODO: is decision true here???
        {true, true, _, _} -> {{branch, 3}, {accept_val, SuperSetVote}}; %%TODO: is decision true here???
        {true, false, _, _} -> {{branch, 3}, {accept_val, abort}}; %%TODO: is decision true here???
        _ -> error
    end.

conflict_exist() ->
    %% TODO: implement the concurrency control
    false.

is_greater_ballot({_N1, _P1}, {_N2, _P2}) -> true.
is_greater_or_equal_ballot({_N1, _P1}, {_N2, _P2}) -> true.

%% -------------
%% HANDOFF
%% -------------

%% a vnode in the handoff lifecycle stage will not accept handle_commands anymore
%% instead every command is redirected to the handle_handoff_command implementations
%% for simplicity, we block every command except the fold handoff itself

%% every key in the vnode will be passed to this function
handle_handoff_command(#riak_core_fold_req_v2{foldfun = FoldFun, acc0 = Acc0}, _Sender,
    State = #state{kv_state = KvState}) ->
    AllKeys = maps:keys(KvState),

    FoldKeys = fun(Key, AccIn) ->
        %% log
        logger:notice("Encoding key ~p for handoff", [Key]),

        %% get the value for the key
        Val = maps:get(Key, KvState),

        %% FoldFun uses encode_handoff_item to serialize the key-value pair and modify the handoff state Acc0
        Acc1 = FoldFun(Key, Val, AccIn),
        Acc1
    end,
	
    %% maps:fold can be used, too
    AccFinal = lists:foldl(FoldKeys, Acc0, AllKeys),

    %% kv store state does not change for this handoff implementation
    {reply, AccFinal, State};

handle_handoff_command(Message, _Sender, State) ->
    logger:warning("handoff command ~p, blocking until handoff is finished", [Message]),
    {reply, {error, processing_handoff}, State}.

handoff_starting(TargetNode, State = #state{partition = Partition}) ->
    logger:notice("handoff starting ~p: ~p", [Partition, TargetNode]),
    {true, State}.

handoff_cancelled(State = #state{partition = Partition}) ->
    logger:notice("handoff cancelled ~p", [Partition]),
    {ok, State}.

handoff_finished(TargetNode, State = #state{partition = Partition}) ->
    logger:notice("handoff finished ~p: ~p", [Partition, TargetNode]),
    {ok, State}.

handle_handoff_data(BinData, State = #state{kv_state = KvState}) ->
    TermData = binary_to_term(BinData),
    {Key, Value} = TermData,
    logger:notice("handoff data received for key ~p", [Key]),
    KvState1 = maps:put(Key, Value, KvState),
    {reply, ok, State#state{kv_state = KvState1}}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

is_empty(State = #state{kv_state = KvState, partition = _Partition}) ->
    IsEmpty= maps:size( KvState) == 0,
    {IsEmpty, State}.


delete(State = #state{partition = Partition, kv_state = #{}}) ->
    logger:debug("Nothing to delete for partition ~p", [Partition]),
    {ok, State#state{kv_state = #{}}};

delete(State = #state{partition = Partition, kv_state = KvState}) ->
    logger:info("delete ~p, ~p keys", [Partition, maps:size(KvState)]),
    {ok, State#state{kv_state = #{}}}.


%% -------------
%% Not needed / not implemented
%% -------------

handle_overload_command(_, _, _) ->
    ok.

handle_overload_info(_, _Idx) ->
    ok.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

