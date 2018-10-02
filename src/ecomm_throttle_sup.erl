-module(ecomm_throttle_sup).

-behaviour(supervisor).

-export([start_link/3, name/1]). %% API
-export([init/1]). %% Supervisor callbacks

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Name, MaxRequestsPerSec, MaxRequestAgeMSec) ->
    supervisor:start_link({local, name(Name)}, ?MODULE, [Name, MaxRequestsPerSec, MaxRequestAgeMSec]).

name(Name) when is_atom(Name) ->
    binary_to_atom(<<"ecomm_throttle_sup_", (atom_to_binary(Name, utf8))/binary>>, utf8);
name(Id) when is_integer(Id) ->
    binary_to_atom(<<"ecomm_throttle_sup_", (integer_to_binary(Id))/binary>>, utf8).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Name, MaxRequestsPerSec, MaxRequestAgeMSec]) ->
    Tab = ets:new(ecomm_throttle:name(Name), [named_table, ordered_set, public]),
    {ok, {{one_for_one, 1, 1},
          [{ecomm_throttle, {ecomm_throttle, start_link, [Tab, MaxRequestsPerSec, MaxRequestAgeMSec]},
            permanent, 2000, worker, [ecomm_throttle]}]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
