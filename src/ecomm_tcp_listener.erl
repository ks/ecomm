-module(ecomm_tcp_listener).

-behaviour(gen_server).

-export([start_link/4, stop/1, name/1]). % API

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]). % callbacks

%%%===================================================================
%%% API
%%%===================================================================

%% CSockFn = fun (ListenSock, ClientSock) -> ..gen_tcp:controlling_process(ClientSocket, NewOwnerPid)..
start_link(Port, Opts, CSockFn, NumAcceptors) 
  when is_integer(Port), 
       is_list(Opts), 
       is_function(CSockFn, 2),
       is_integer(NumAcceptors), NumAcceptors > 0 ->
    gen_server:start_link({local, name(Port)}, ?MODULE, [Port, Opts, CSockFn, NumAcceptors], []).

stop(Listener) ->
    gen_server:call(Listener, stop).

name(Port) ->
    binary_to_atom(<<"ecomm_tcp_listener_", (integer_to_binary(Port))/binary>>, utf8).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Port, Opts, CSockFn, 1]) ->
    {ok, {Port, Opts, CSockFn, 1}};
init([Port, Opts, CSockFn, NumAcceptors]) ->
    Opts1 = [{reuseaddr, true} | ecomm_pl:del(reuseaddr, Opts)],
    {ok, {Port, Opts1, CSockFn, NumAcceptors}}.

handle_cast({acceptors_sup, AccSup}, {Port, Opts, CSockFn, NumAcceptors}) ->
    {ok, LSock} = gen_tcp:listen(Port, Opts),
    [ecomm_tcp_acceptors_sup:add_acceptor(AccSup, LSock, CSockFn, {Port, Index}) ||
	Index <- lists:seq(1, NumAcceptors)],
    {noreply, {LSock, Port}};
handle_cast(_Msg, State) -> {noreply, State}.

terminate(Reason, {LSock, Port}) ->
    error_logger:info_msg("TCP listener on port ~p died: ~p", [Port, Reason]),
    catch gen_tcp:close(LSock).

handle_call(_Request, _From, State) -> {reply, ignored, State}.
handle_info(_Msg, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
