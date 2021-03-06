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

-module(gpac_test).

%% tests
-export([
    test/0
]).

%%-define(BUCKET, test_utils:bucket(simple_kv_bucket)).
-define(BUCKET, simple_kv_bucket).

test() ->
	{ok, Pid} = gpac_leader_sup:start_fsm(),
	TxId = 1,
	Bucket = ?BUCKET,
	io:format("Bucket = ~p\n", [Bucket]),
	Res1 = gen_statem:call(Pid, {start_tx, TxId}),
	io:format("Res1 = ~p\n", [Res1]),
	{ok, PrefList1} = gen_statem:call(Pid, {tx_put, Bucket, key1, value1}),
	{ok, PrefList2} = gen_statem:call(Pid, {tx_put, Bucket, key2, value2}),
	{ok, PrefList3, Value} = gen_statem:call(Pid, {tx_get, Bucket, key2}),
	
	io:format("PrefList1 = ~p\n", [PrefList1]),
	io:format("PrefList2 = ~p\n", [PrefList2]),
	io:format("PrefList3 = ~p\n", [PrefList3]),
	io:format("Value of key2 = ~p\n", [Value]).
%%	Res3 = gen_statem:call(Pid, {elect_and_prepare}),
%%    io:format("Res3 = ~p\n", [Res3]),
%%	Res4 = case Res3 of
%%			ok -> gen_statem:call(Pid, {commit});
%%			abort -> gen_statem:call(Pid, {abort})
%%		end,
%%   io:format("Res4 = ~p\n", [Res4]).



