-module(ecomm_tcps_sup).

-behaviour(supervisor).

-export([start_link/0, add_sup/4, add_sup/6, del_sup/1]). %% API

-export([init/1]). %% Supervisor callbacks

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_sup(Port, Opts, CSockFn, NumAcceptors) ->
    supervisor:start_child(?MODULE, [Port, Opts, CSockFn, NumAcceptors]).

add_sup(Port, Opts, CSockFn, NumAcceptors, MaxReqsPerSec, MaxReqAgeMSec) ->
    supervisor:start_child(?MODULE, [Port, Opts, CSockFn, NumAcceptors, MaxReqsPerSec, MaxReqAgeMSec]).

del_sup(SupPid) ->
    supervisor:terminate_child(?MODULE, SupPid).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    Spec = [{ecomm_tcp_sup,
	     {ecomm_tcp_sup, start_link, []}, permanent, 2000, supervisor, 
	     [ecomm_tcp_sup]}],
    {ok, {{simple_one_for_one, 1, 1}, Spec}}.
