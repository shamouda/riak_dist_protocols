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

-module(sim2pc_cohort_vnode).

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

%assuming we handle just one request at a time
-record(state, {
    partition, 
    kv_state,  %stable k/v values (committed values)
    kv_pending, %non-committed changes  ReqId -> K/V Map 
    prepared  %prepared transactions
}).

-spec start_vnode(integer()) -> any().
start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%who will give the partition parameter
init([Partition]) ->
    {ok, #state {  partition = Partition, 
                   kv_state = maps:new(), 
                   kv_pending = maps:new(), 
                   prepared = sets:new() }}.

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

handle_command({prepare, ReqId}, _Sender, State = #state{prepared = Prepared, partition = Partition}) ->
    Location = [Partition, node()],
    logger:info("handle_command(prepare) here=~p", [Location]),
    AlreadyPrepared = sets:is_element(ReqId, Prepared),
    prepare(AlreadyPrepared, ReqId, Location, State);

handle_command({commit, ReqId}, _Sender, State = #state{prepared = Prepared, kv_pending = PendingState, kv_state = KvState, partition = Partition}) ->
    Location = [Partition, node()],
    logger:info("handle_command(commit) here=~p", [Location]),
    NewPrep = sets:del_element(ReqId, Prepared),
    {WriteMap, _ReadSet} = maps:get(ReqId, PendingState),
    NewKvState = maps:fold(fun(K, V, Accum) -> maps:put(K, V, Accum) end, KvState, WriteMap),
    NewPendingState = maps:remove(ReqId, PendingState),
    {reply, {{request_id, ReqId}, {location, Location}}, State#state{kv_pending = NewPendingState, kv_state = NewKvState, prepared = NewPrep}};

handle_command({abort, ReqId}, _Sender, State = #state{prepared = Prepared, kv_pending = PendingState, partition = Partition}) ->
    Location = [Partition, node()],
    logger:info("handle_command(abort) here=~p", [Location]),
    NewPrep = sets:del_element(ReqId, Prepared),
    NewPendingState = maps:remove(ReqId, PendingState),
    {reply, {{request_id, ReqId}, {location, Location}}, State#state{kv_pending = NewPendingState, prepared = NewPrep}};

handle_command(Message, _Sender, State) ->
    logger:warning("unhandled_command ~p", [Message]),
    {noreply, State}.

%%%%%%% Utility functions %%%%%%%  
append_pending_values(State, PendingState, ReqId, Key, Value, Partition) ->
    Location = [Partition, node()],
    {WriteMap, ReadSet} = maps:get(ReqId, PendingState, {maps:new(), sets:new()}),
    WriteMapAfter = maps:put(Key, Value, WriteMap),
    ReadSetAfter = case sets:is_element(Key, ReadSet) of
        true -> sets:del_element(Key, ReadSet);
        false -> ReadSet
    end,
    NewPendingState = maps:put(ReqId, {WriteMapAfter, ReadSetAfter}, PendingState),
    {reply, {{request_id, ReqId}, {location, Location}}, State#state{kv_pending = NewPendingState}}.

read_value(State, PendingState, KvState, ReqId, Key, Partition) ->
    Location = [Partition, node()],
    {WriteMap, ReadSet} = maps:get(ReqId, PendingState, {maps:new(), sets:new()}),
    Value = case maps:is_key(Key, WriteMap) of
        true -> maps:get(Key, WriteMap);
        false -> maps:get(Key, KvState, not_assigned)
    end,
    ReadSetAfter = case sets:is_element(Key, ReadSet) of
        true -> ReadSet;
        false -> sets:add_element(Key, ReadSet)
    end,
    NewPendingState = maps:put(ReqId, {WriteMap, ReadSetAfter}, PendingState),
    {reply, {{request_id, ReqId}, {value, Value}, {location, Location}}, State#state{kv_pending = NewPendingState}}.

%% prepare an already prepared transaction
prepare(_AlreadyPrepared = true, ReqId, Location, State) ->
    {reply, {{request_id, ReqId}, {location, Location}}, State};
%% prepare a non-prepared transaction
prepare(_AlreadyPrepared = false, ReqId, Location, State = #state{prepared = Prepared, kv_pending = PendingState}) ->
    case conflict_exist() of
        true -> 
            NewPending = maps:remove(ReqId, PendingState),
            {reply, abort, State#state{kv_pending = NewPending}};
        false -> 
            NewPrep = sets:add_element(ReqId, Prepared),
            {reply, {{request_id, ReqId}, {location, Location}}, State#state{prepared = NewPrep}}
    end.

conflict_exist() ->
    %% TODO: implement the concurrency control
    false.
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

