-module(ecomm_tcp_acceptors_sup).

-behaviour(supervisor).

-export([start_link/1, add_acceptor/4, delete_acceptor/2, name/1]). % API

-export([init/1]). %% Supervisor callbacks

%%%===================================================================
%%% API
%%%===================================================================

start_link(Port) ->
    supervisor:start_link({local, name(Port)}, ?MODULE, [Port]).

add_acceptor(Sup, ListenerSocket, ClientSocketFn, {Port, Index}) ->
    supervisor:start_child(Sup, [ListenerSocket, ClientSocketFn, {Port, Index}]).

delete_acceptor(Sup, Pid) ->
    supervisor:terminate_child(Sup, Pid).

name(Port) ->
    binary_to_atom(<<"ecomm_tcp_acceptors_sup_", (integer_to_binary(Port))/binary>>, utf8).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Port]) ->
    gen_server:cast(ecomm_tcp_listener:name(Port), {acceptors_sup, self()}),
    Spec = [{ecomm_tcp_acceptor,
	     {ecomm_tcp_acceptor, start_link, []},
	     permanent, 2000, worker, [ecomm_tcp_acceptor]}],
    {ok, {{simple_one_for_one, 1, 1}, Spec}}.

