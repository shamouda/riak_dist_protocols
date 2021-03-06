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

-module(simple_kv_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    Sim2PC_Cohort = {sim2pc_cohort_vnode_master,
                     {riak_core_vnode_master, start_link, [sim2pc_cohort_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},
    
    Sim2PC_Coord = {sim2pc_coord_sup,
                   {sim2pc_coord_sup, start_link, []},
                   permanent, 5000, supervisor, [sim2pc_coord_sup]},
    
    GPAC_Cohort = {gpac_cohort_vnode_master,
                     {riak_core_vnode_master, start_link, [gpac_cohort_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},
    
    GPAC_Coord = {gpac_leader_sup,
                   {gpac_leader_sup, start_link, []},
                   permanent, 5000, supervisor, [gpac_leader_sup]},

    {ok, {{one_for_one, 5, 10}, [ Sim2PC_Cohort, Sim2PC_Coord, 
                                GPAC_Cohort, GPAC_Coord ]}}.
