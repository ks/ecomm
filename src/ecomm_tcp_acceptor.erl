-module(ecomm_tcp_acceptor).

-behaviour(gen_server).

-export([start_link/3, name/1]). %% API

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]). % callbacks

%%%===================================================================
%%% API
%%%===================================================================

start_link(ListenerSocket, ClientSocketFn, {Port, Index}) ->
    Name = name({Port, Index}),
    gen_server:start_link({local, Name}, ?MODULE, [ListenerSocket, ClientSocketFn], []).

name({Port, Index}) ->
    [PortBin, IndexBin] = [integer_to_binary(X) || X <- [Port, Index]],
    binary_to_atom(<<"ecomm_tcp_acceptor_", PortBin/binary, "_", IndexBin/binary>>, utf8).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([LSock, CSockFn]) ->
    gen_server:cast(self(), start_accepting),
    {ok, {LSock, CSockFn}}.

handle_cast(start_accepting, {LSock, CSockFn}) ->
    acceptor_loop(LSock, CSockFn),
    {noreply, {LSock, CSockFn}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) -> {reply, ignored, State}.
handle_info(_, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

acceptor_loop(LSock, CSockFn) ->
    case gen_tcp:accept(LSock) of
	{ok, CSock} ->
            %% CSockFn MUST disown client socket (via gen_tcp:controlling_process)
	    CSockFn(LSock, CSock),
	    acceptor_loop(LSock, CSockFn);
	{error, Reason} ->
	    error({ecomm_tcp_acceptor, Reason})
    end.
