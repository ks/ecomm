-module(ecomm_conn_mgr).

-export([enable/1, enable_throttled/2, disable/1,
         list_connections/0, list_connections/1]). % API

-export([default_conn_stat/1, default_codec/1, default_err_report/1, echo_app_handler/1]).

-export([echo_test/2,
         echo_dump_test/3,
         throttled_echo_test/4,
         throttled_echo_dump_test/5,
	 echo_callbacks/0,        %% testing - sends back everything
	 echo_dump_callbacks/1, %% testing - also dumps to file
         lg_test/2]).

-define(DEFAULT_TCP_ACCEPTORS, 2).

%%%==============================================
%%% Connection manager API
%%%==============================================

enable({Protocol, Port, Options, Callbacks}) ->
    enable1({Protocol, Port, Options, parse_callbacks(Callbacks)});
enable({Protocol, Port, Options, Callbacks, AppState}) ->
    enable1({Protocol, Port, Options, parse_callbacks(Callbacks), AppState}).

def_cstat() -> {?MODULE, default_conn_stat}.
def_codec() -> {?MODULE, default_codec}.
def_error() -> {?MODULE, default_err_report}.

parse_callbacks(AppHandler) when is_function(AppHandler, 1) ->
    {def_cstat(), def_codec(), AppHandler, def_error()};
parse_callbacks({AppHandler}) ->
    {def_cstat(), def_codec(), ecomm_fn:ensure(AppHandler, 1), def_error()};
parse_callbacks({M, F}) when is_atom(M), is_atom(F) ->
    {def_cstat(), def_codec(), ecomm_fn:ensure({M, F}, 1), def_error()};
parse_callbacks({Codec, AppHandler}) ->
    {def_cstat(), ecomm_fn:ensure(Codec, 1), ecomm_fn:ensure(AppHandler, 1), def_error()};
parse_callbacks({ConnStat, Codec, AppHandler}) ->
    {ecomm_fn:ensure(ConnStat, 1), ecomm_fn:ensure(Codec, 1), ecomm_fn:ensure(AppHandler, 1), def_error()};
parse_callbacks({ConnStat, Codec, AppHandler, ErrReport}) ->
    {ecomm_fn:ensure(ConnStat, 1), ecomm_fn:ensure(Codec, 1), ecomm_fn:ensure(AppHandler, 1), ecomm_fn:ensure(ErrReport, 1)};
parse_callbacks(_) ->
    error({badarg, invalid_callbacks}).


%% TCPOpts - options to gen_tcp:listen ++ [{num_acceptors, integer()}]
enable1({tcp, ListenPort, TCPOpts, Callbacks}) ->
    enable1({tcp, ListenPort, TCPOpts, Callbacks, []});
enable1({tcp, ListenPort, TCPOpts, Callbacks, AppState}) ->
    enable_tcp(ListenPort, TCPOpts, Callbacks, AppState);

enable1({udp, RecvPort, UDPOpts, Callbacks}) ->
    enable1({udp, RecvPort, UDPOpts, Callbacks, []});
enable1({udp, RecvPort, UDPOpts, Callbacks, AppState}) ->
    enable_udp(RecvPort, UDPOpts, Callbacks, AppState).



enable_throttled({Protocol, Port, Options, Callbacks}, undefined) ->
    enable({Protocol, Port, Options, Callbacks});
enable_throttled({Protocol, Port, Options, Callbacks}, {MaxReqsPerSec, MaxReqAgeMSec}) ->
    enable_throttled1({Protocol, Port, Options, parse_callbacks(Callbacks)},
                      {MaxReqsPerSec, MaxReqAgeMSec}).

enable_throttled1({tcp, ListenPort, TCPOpts, Callbacks}, {MaxReqsPerSec, MaxReqAgeMSec}) ->
    enable_throttled1({tcp, ListenPort, TCPOpts, Callbacks, []}, {MaxReqsPerSec, MaxReqAgeMSec});
enable_throttled1({tcp, ListenPort, TCPOpts, Callbacks, AppState}, {MaxReqsPerSec, MaxReqAgeMSec}) ->
    enable_throttled_tcp(ListenPort, TCPOpts, Callbacks, AppState, MaxReqsPerSec, MaxReqAgeMSec);

enable_throttled1({udp, RecvPort, UDPOpts, Callbacks}, {MaxReqsPerSec, MaxReqAgeMSec}) ->
    enable_throttled1({udp, RecvPort, UDPOpts, Callbacks, []}, {MaxReqsPerSec, MaxReqAgeMSec});
enable_throttled1({udp, RecvPort, UDPOpts, Callbacks, AppState}, {MaxReqsPerSec, MaxReqAgeMSec}) ->
    enable_throttled_udp(RecvPort, UDPOpts, Callbacks, AppState, MaxReqsPerSec, MaxReqAgeMSec).


disable({tcp, ListenPort}) ->
    disable_tcp(ListenPort);
disable({udp, RecvPort}) ->
    disable_udp(RecvPort).


list_connections() ->
    list_connections(udp) ++ list_connections(tcp).

list_connections(Proto) when Proto == udp; Proto == tcp ->
    [begin
         {registered_name, Name} = process_info(Pid, registered_name),
         NameBin = atom_to_binary(Name, utf8),
         NameSz = size(NameBin),
         Pos = scanr_bin(fun ($\_, Idx) -> {stop, Idx};
                             (_, Idx) -> {ok, Idx - 1}
                         end, NameBin, NameSz - 1),
         {Proto, binary_to_integer(binary:part(NameBin, Pos + 1, NameSz - Pos - 1))}
     end || {_, Pid, _, _} <- supervisor:which_children(
                                if Proto == udp -> ecomm_udps_sup;
                                   Proto == tcp -> ecomm_tcps_sup
                                end)].

%%%%%%%%%%

ensure_tcp_options(TCPOpts) ->
    TCPOpts1 = ecomm_pl:del(num_acceptors, TCPOpts),
    NumAcceptors = ecomm_pl:getv(num_acceptors, TCPOpts, ?DEFAULT_TCP_ACCEPTORS),
    case ecomm_conn_tcp:ensure_options({listen, TCPOpts1}) of
	{ok, TCPOpts2} ->
	    {ok, TCPOpts2, NumAcceptors};
	{error, Reason} ->
	    {error, Reason}
    end.

enable_tcp(ListenPort, TCPOpts,
           {_ConnStat, _Codec, _AppHandler, _ErrReport} = Callbacks,
           AppState)
  when is_integer(ListenPort), is_list(TCPOpts), is_list(AppState) ->
    case ensure_tcp_options(TCPOpts) of
	{ok, TCPOpts1, NumAcceptors} ->
	    CSockFn = fun (LSock, CSock) ->
			      start_tcp_handler({LSock, CSock}, Callbacks, AppState)
		      end,
	    ecomm_tcps_sup:add_sup(ListenPort, TCPOpts1, CSockFn, NumAcceptors);
	{error, Reason} ->
	    {error, Reason}
    end.

enable_throttled_tcp(ListenPort, TCPOpts,
                     {_ConnStat, _Codec, _AppHandler, _ErrReport} = Callbacks,
                     AppState, MaxReqsPerSec, MaxReqAgeMSec)
  when is_integer(ListenPort),
       is_list(TCPOpts), is_list(AppState),
       is_integer(MaxReqsPerSec), is_integer(MaxReqAgeMSec) ->
    case ensure_tcp_options(TCPOpts) of
	{ok, TCPOpts1, NumAcceptors} ->
            Throttler = ecomm_throttle:name(ListenPort),
	    CSockFn = fun (LSock, CSock) ->
			      start_tcp_handler({LSock, CSock}, Callbacks, AppState, Throttler)
		      end,
	    ecomm_tcps_sup:add_sup(ListenPort, TCPOpts1, CSockFn, NumAcceptors,
                                   MaxReqsPerSec, MaxReqAgeMSec);
	{error, Reason} ->
	    {error, Reason}
    end.


start_tcp_handler({LSock, CSock}, Callbacks, AppState) ->
    {ok, HandlerPid} = ecomm_conn_tcp:start(Callbacks, AppState),
    init_tcp_handler(HandlerPid, {LSock, CSock}).

start_tcp_handler({LSock, CSock}, Callbacks, AppState, Throttler) ->
    {ok, HandlerPid} = ecomm_conn_tcp:start(Callbacks, AppState, Throttler),
    init_tcp_handler(HandlerPid, {LSock, CSock}).

init_tcp_handler(HandlerPid, {LSock, CSock}) ->
    erlang:send(HandlerPid, {tcp_opened, {LSock, CSock}}),
    case gen_tcp:controlling_process(CSock, HandlerPid) of
        ok ->
            ok;
	{error, closed} ->
	    ecomm_conn_tcp:stop(HandlerPid);
	{error, Reason} ->
	    ecomm_conn_tcp:stop(HandlerPid),
	    error({ecomm_tcp_ownership, Reason})
    end.


disable_tcp(ListenPort) when is_integer(ListenPort) ->
    case whereis(ecomm_tcp_sup:name(ListenPort)) of
	undefined ->
	    {error, not_found};
	Sup ->
	    supervisor:terminate_child(ecomm_tcps_sup, Sup)
    end.


%%%%%%%%%%

ensure_udp_options(UDPOpts) ->
    ecomm_conn_udp:ensure_options({open, UDPOpts}).

enable_udp(RecvPort, UDPOpts,
           {_ConnStat, _Codec, _AppHandler, _ErrReport} = Callbacks,
           AppState)
  when is_integer(RecvPort),
       is_list(UDPOpts), is_list(AppState) ->
    case ensure_udp_options(UDPOpts) of
	{ok, UDPOpts1} ->
	    PacketFn = fun ({LSock, Peer, Packet}) ->
			       start_udp_handler({LSock, Peer}, Packet, Callbacks, AppState)
		       end,
	    ecomm_udps_sup:add_sup(RecvPort, UDPOpts1, PacketFn);
	{error, Reason} ->
	    {error, Reason}
    end.

enable_throttled_udp(RecvPort, UDPOpts,
                     {_ConnStat, _Codec, _AppHandler, _ErrReport} = Callbacks,
                     AppState, MaxReqsPerSec, MaxReqAgeMSec)
  when is_integer(RecvPort),
       is_list(UDPOpts), is_list(AppState),
       is_integer(MaxReqsPerSec), is_integer(MaxReqAgeMSec) ->
    case ensure_udp_options(UDPOpts) of
	{ok, UDPOpts1} ->
            Throttler = ecomm_throttle:name(RecvPort),
	    PacketFn = fun ({LSock, Peer, Packet}) ->
			       start_udp_handler({LSock, Peer}, Packet, Callbacks, AppState, Throttler)
		       end,
	    ecomm_udps_sup:add_sup(RecvPort, UDPOpts1, PacketFn, MaxReqsPerSec, MaxReqAgeMSec);
	{error, Reason} ->
	    {error, Reason}
    end.

start_udp_handler({LSock, Peer}, Packet, Callbacks, AppState) ->
    {ok, HandlerPid} = ecomm_conn_udp:start(Callbacks, AppState),
    init_udp_handler(HandlerPid, {LSock, Peer}, Packet).

start_udp_handler({LSock, Peer}, Packet, Callbacks, AppState, Throttler) ->
    {ok, HandlerPid} = ecomm_conn_udp:start(Callbacks, AppState, Throttler),
    init_udp_handler(HandlerPid, {LSock, Peer}, Packet).

init_udp_handler(HandlerPid, {LSock, Peer}, Packet) ->
    HandlerPid ! {udp, {LSock, Peer}, Packet}.


disable_udp(RecvPort) when is_integer(RecvPort) ->
    case whereis(ecomm_udp_sup:name(RecvPort)) of
	undefined ->
	    {error, not_found};
	Sup ->
	    supervisor:terminate_child(ecomm_udps_sup, Sup)
    end.


%%%%=============================================
%%%% UTILS
%%%%=============================================

scanr_bin(_F, <<>>, Init) -> Init;
scanr_bin(F, Bin, Init) ->
    scanr_bin(F, Bin, Init, size(Bin) - 1).
scanr_bin(_F, _Bin, State, Idx) when Idx =< 0 -> State;
scanr_bin(_F, <<>>, Init, _Idx) -> Init;
scanr_bin(F, Bin, State, Idx) ->
    case F(binary:at(Bin, Idx), State) of
        {stop, State1} -> State1;
        {ok, State1} -> scanr_bin(F, Bin, State1, Idx - 1);
        ok -> scanr_bin(F, Bin, State, Idx - 1)
    end.

%%%==============================================
%%% Default pass-through callbacks
%%%==============================================

default_conn_stat({start, _Protocol, AppState}) -> {ok, AppState};
default_conn_stat({stop, _Protocol, _Reason, _AppState}) -> ok.

default_codec({decode, Packet}) when is_binary(Packet) -> {ok, Packet};
default_codec({encode, Packet}) when is_binary(Packet)-> {ok, Packet};
default_codec({encode, {overload, _Packet}}) ->
    {ok, <<"!!! OVERLOAD at ", (ecomm_test:format_udate_bin())/binary, " !!!">>}.

default_err_report({Ex, Err, Stacktrace, State}) ->
    io:format("!!!!!!!!!! ecomm_conn_mgr: ~p ~p~nstacktrace: ~p~nlast state: ~p~n",
              [Ex, Err, Stacktrace, State]),
    exit({Ex, Err}). % let failed conn die

%%%==============================================
%%% Testing callbacks
%%%==============================================

echo_app_handler({Packet, _AppState}) -> {ok, Packet}.

echo_callbacks() ->
    {{?MODULE, default_conn_stat}, {?MODULE, default_codec}, {?MODULE, echo_app_handler}}.

echo_dump_callbacks(ToFilename) ->
    {{?MODULE, default_conn_stat}, {?MODULE, default_codec},
     fun ({Line, _AppState}) ->
	     ok = file:write_file(ToFilename, Line, [append]),
	     {ok, Line}
     end}.

%%%%%%%%%%

echo_test(Proto, Port) when Proto == tcp; Proto == udp ->
    enable({Proto, Port, [], echo_callbacks()}).
throttled_echo_test(Proto, Port, MaxReqsPerSec, MaxReqAgeMSec)
  when Proto == tcp; Proto == udp ->
    enable_throttled({Proto, Port, [], echo_callbacks()}, {MaxReqsPerSec, MaxReqAgeMSec}).

echo_dump_test(Proto, Port, Filename) when Proto == tcp; Proto == udp ->
    enable({Proto, Port, [], echo_dump_callbacks(Filename)}).
throttled_echo_dump_test(Proto, Port, Filename, MaxReqsPerSec, MaxReqAgeMSec)
  when Proto == tcp; Proto == udp ->
    enable_throttled({Proto, Port, [], echo_dump_callbacks(Filename)}, {MaxReqsPerSec, MaxReqAgeMSec}).


%%%%================================================================================
%%%% LOAD GENERATION test
%%%%================================================================================

lg_test(Proto, Port) when Proto == udp; Proto == tcp ->
    throttled_echo_test(Proto, Port, 10, 2000),
    F = fun (ProcId, ReqId) ->
                Msg = iolist_to_binary(io_lib:format("proc [~p] req ~p", [ProcId, ReqId])),
                io:format("~p~n", [ecomm_test:send_recv(Proto, {{127,0,0,1}, Port}, Msg)])
        end,
    ecomm_test:gen_load(F, 24, 5000, 200).
