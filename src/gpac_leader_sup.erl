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

-module(gpac_leader_sup).

-behavior(supervisor).

-export([
    start_fsm/0,
    start_link/0
]).

-export([
    init/1
]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Starts a new transaction coordinator under this supervisor
start_fsm() ->
    % calls gpac_leader:start_link()
    supervisor:start_child(?MODULE, []).

%% Starts the coordinator of a transaction.
init([]) ->
    Worker = {undefined,
        {gpac_leader, start_link, []},
        temporary, 5000, worker, [gpac_leader]},
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.
