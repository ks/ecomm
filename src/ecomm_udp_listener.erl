-module(ecomm_udp_listener).

-behaviour(gen_server).

-export([start_link/3, name/1]). % API

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]). % callbacks

%%%===================================================================
%%% API
%%%===================================================================

start_link(Port, Opts, PacketFn)
  when is_integer(Port),
       is_list(Opts), 
       is_function(PacketFn, 1) ->
    gen_server:start_link({local, name(Port)}, ?MODULE, [Port, Opts, PacketFn], []).

name(Port) ->
    binary_to_atom(<<"ecomm_udp_listener_", (integer_to_binary(Port))/binary>>, utf8).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Port, Opts, PacketFn]) ->
    {ok, LSock} = gen_udp:open(Port, Opts),
    {ok, {LSock, PacketFn}}.

handle_info({udp, LSock, IP, InPortNo, Packet}, {LSock, PacketFn}) ->
    PacketFn({LSock, {IP, InPortNo}, Packet}),
    {noreply, {LSock, PacketFn}};
handle_info(_Msg, State) -> 
    {noreply, State}.

terminate(_Reason, {LSock, _PacketFn}) ->
    catch gen_udp:close(LSock).

handle_cast(_Msg, State) -> {noreply, State}.
handle_call(_Request, _From, State) -> {reply, ignored, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
