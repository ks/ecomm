-module(ecomm_tcp_sup).

-behaviour(supervisor).

-export([start_link/4, start_link/6, name/1]). % API

-export([init/1]). %% Supervisor callbacks

%%%===================================================================
%%% API
%%%===================================================================

start_link(Port, Opts, CSockFn, NumAcceptors) ->
    supervisor:start_link({local, name(Port)}, ?MODULE, [Port, Opts, CSockFn, NumAcceptors]).

start_link(Port, Opts, CSockFn, NumAcceptors, MaxReqsPerSec, MaxReqAgeMSec) ->
    supervisor:start_link({local, name(Port)}, ?MODULE, 
                          [Port, Opts, CSockFn, NumAcceptors, MaxReqsPerSec, MaxReqAgeMSec]).

name(Port) ->
    binary_to_atom(<<"ecomm_tcp_sup_", (integer_to_binary(Port))/binary>>, utf8).
    
%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Port, Opts, CSockFn, NumAcceptors]) ->
    {ok, {{rest_for_one, 1, 1}, common_specs(Port, Opts, CSockFn, NumAcceptors)}};
	   
init([Port, Opts, CSockFn, NumAcceptors, MaxReqsPerSec, MaxReqAgeMSec]) ->
    {ok, {{rest_for_one, 1, 1},
          [{ecomm_throttle_sup,
            {ecomm_throttle_sup, start_link, [Port, MaxReqsPerSec, MaxReqAgeMSec]},
            permanent, 2000, supervisor, [ecomm_throttle_sup]} |
           common_specs(Port, Opts, CSockFn, NumAcceptors)]}}.
	   
%%%===================================================================
%%% Internal functions
%%%===================================================================

common_specs(Port, Opts, CSockFn, NumAcceptors) ->
    [{ecomm_tcp_listener,
      {ecomm_tcp_listener, start_link, [Port, Opts, CSockFn, NumAcceptors]},
      permanent, 2000, worker, [ecomm_tcp_listener]},
     {ecomm_tcp_acceptors_sup,
      {ecomm_tcp_acceptors_sup, start_link, [Port]},
      permanent, 2000, supervisor, [ecomm_tcp_acceptors_sup]}].
