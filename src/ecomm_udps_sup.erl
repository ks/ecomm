-module(ecomm_udps_sup).

-behaviour(supervisor).

-export([start_link/0, add_sup/3, add_sup/5, del_sup/1]). %% API

-export([init/1]). %% Supervisor callbacks

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_sup(Port, Opts, PacketFn) ->
    supervisor:start_child(?MODULE, [Port, Opts, PacketFn]).

add_sup(Port, Opts, PacketFn, MaxReqsPerSec, MaxReqAgeMSec) ->
    supervisor:start_child(?MODULE, [Port, Opts, PacketFn, MaxReqsPerSec, MaxReqAgeMSec]).

del_sup(SupPid) ->
    supervisor:terminate_child(?MODULE, SupPid).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    Spec = [{ecomm_udp_sup,
	     {ecomm_udp_sup, start_link, []}, permanent, 2000, supervisor,
	     [ecomm_udp_sup]}],
    {ok, {{simple_one_for_one, 1, 1}, Spec}}.
