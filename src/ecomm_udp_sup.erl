-module(ecomm_udp_sup).

-behaviour(supervisor).

-export([start_link/3, start_link/5, name/1]). % API

-export([init/1]). %% Supervisor callbacks

%%%===================================================================
%%% API
%%%===================================================================

start_link(Port, Opts, PacketFn) ->
    supervisor:start_link({local, name(Port)}, ?MODULE, [Port, Opts, PacketFn]).

start_link(Port, Opts, PacketFn, MaxReqsPerSec, MaxReqAgeMSec) ->
    supervisor:start_link({local, name(Port)}, ?MODULE, 
                          [Port, Opts, PacketFn, MaxReqsPerSec, MaxReqAgeMSec]).

name(Port) ->
    binary_to_atom(<<"ecomm_udp_sup_", (integer_to_binary(Port))/binary>>, utf8).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Port, Opts, PacketFn]) ->
    {ok, {{one_for_one, 1, 1}, common_specs(Port, Opts, PacketFn)}};

init([Port, Opts, PacketFn, MaxReqsPerSec, MaxReqAgeMSec]) ->
    {ok, {{one_for_one, 1, 1}, 
          [{ecomm_throttle_sup,
            {ecomm_throttle_sup, start_link, [Port, MaxReqsPerSec, MaxReqAgeMSec]},
            permanent, 2000, supervisor, [ecomm_throttle_sup]} |
           common_specs(Port, Opts, PacketFn)]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

common_specs(Port, Opts, PacketFn) ->
    [{ecomm_udp_listener,
      {ecomm_udp_listener, start_link, [Port, Opts, PacketFn]}, permanent, 2000, worker,
      [ecomm_udp_listener]}].
