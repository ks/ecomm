-module(ecomm_conn_mgr_sup).

-behaviour(supervisor).

-export([start_link/0]). % API

-export([kill/0]). % shell testing, development only

-export([init/1]). %% Supervisor callbacks

%% supervision tree looks as:
%% (example for one TCP on port 11111 (with 2 acceptors) and one UDP on port 22222):
%%
%%                module               |              name               |             strategy 
%%
%% ecomm_conn_mgr_sup                  |  ecomm_conn_mgr_sup             |  one_for_one
%% |- ecomm_tcps_sup                   |  ecomm_tcps_sup                 |  simple_one_for_one
%% |  |- ecomm_tcp_sup                 |  ecomm_tcp_sup_11111            |  rest_for_one
%% |     |- ecomm_tcp_listener         |  ecomm_tcp_listener_11111       |  (worker)
%% |     |- ecomm_tcp_acceptors_sup    |  ecomm_tcp_acceptors_sup_11111  |  simple_one_for_one
%% |        |- ecomm_tcp_acceptor      |  ecomm_tcp_acceptor_11111_1     |  (worker)
%% |        |- ecomm_tcp_acceptor      |  ecomm_tcp_acceptor_11111_2     |  (worker)
%% |- ecomm_udps_sup                   |  ecomm_udps_sup                 |  one_for_one
%%    |- ecomm_udp_sup                 |  ecomm_udp_sup_22222            |  one_for_one
%%       |- ecomm_udp_listener         |  ecomm_udp_listener_22222       |  (worker)

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

kill() ->
    exit(whereis(?MODULE), kill).
    
%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    Specs = [{ecomm_tcps_sup,
	      {ecomm_tcps_sup, start_link, []},
	      permanent, 2000, supervisor, [ecomm_tcps_sup]},
	     {ecomm_udps_sup,
	      {ecomm_udps_sup, start_link, []},
	      permanent, 2000, supervisor, [ecomm_udps_sup]}],
    {ok, {{one_for_one, 1, 1}, Specs}}.
