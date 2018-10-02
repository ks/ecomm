%%%===================================================================
%%% This module is NOT to be used directly, but via:
%%%
%%% ecomm_conn_mgr:enable/1
%%% ecomm_conn_mgr:disable/1
%%%===================================================================
-module(ecomm_conn_udp).

-behaviour(gen_server).

%% Internal API
-export([ensure_options/1, start/2, start/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]). % callbacks

%% internal use only
-record(ecomm_conn_udp,
	{listen_socket :: gen_udp:socket(),
	 peer          :: {inet:host(), inet:portnr()},
	 dec_state     :: binary() | any(),
	 conn_stat     :: {atom(), atom()} | function(),
	 codec         :: {atom(), atom()} | function(),
	 app_handler   :: {atom(), atom()} | function(),
         err_report    :: {atom(), atom()} | function(),
	 state         :: term(),
         throttler     :: undefined | atom()}).

%%%===================================================================
%%% Internal API
%%%===================================================================

start({_ConnStat, _Codec, _AppHandler, _ErrReport} = Callbacks, AppState) when is_list(AppState) ->
    gen_server:start(?MODULE, [Callbacks, AppState], []).
start({_ConnStat, _Codec, _AppHandler, _ErrReport} = Callbacks, AppState, Throttler) 
  when is_list(AppState), is_atom(Throttler) ->
    gen_server:start(?MODULE, [Callbacks, AppState, Throttler], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([{ConnStat, Codec, AppHandler, ErrReport}, AppState]) ->
    {ok, #ecomm_conn_udp{dec_state = ecomm_fn:apply(Codec, [{decode, init}]),
                         conn_stat = ConnStat, 
			 codec = Codec,
			 app_handler = AppHandler,
                         err_report = ErrReport,
			 state = AppState}};
init([{ConnStat, Codec, AppHandler, ErrReport}, AppState, Throttler]) ->
    {ok, S} = init([{ConnStat, Codec, AppHandler, ErrReport}, AppState]),
    {ok, S#ecomm_conn_udp{throttler = Throttler}}.

handle_info({udp, {LSock, Peer}, Packet}, S) ->
    S1 = S#ecomm_conn_udp{listen_socket = LSock, peer = Peer},
    handle_data(Packet, S1).

terminate(Reason, #ecomm_conn_udp{conn_stat = ConnStat, state = AppState}) ->
    conn_stop(ConnStat, Reason, AppState).

handle_call(_Request, _From, State) -> {reply, ignored, State}.
handle_cast(_, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% UDP transport handling
%%%===================================================================

ensure_options({open, Opts}) ->
    BadKeys = gb_sets:from_list([list, binary, active, fd, udp_module, mode,
				 add_membership, drop_membership,
				 multicast_if, multicast_loop, multicast_ttl]),
    OptsSet = gb_sets:from_list(proplists:get_keys(Opts)),
    case gb_sets:to_list(gb_sets:intersection(OptsSet, BadKeys)) of
	[] -> {ok, [binary, {active, true} | Opts]};
	_ -> {error, bad_options}
    end.

%%%%%%%%%% result of handle_data function is result of gen_server:handle_info function

-define(report_err(Body, AppState, S), 
        (try Body of 
             R -> R 
         catch
             Ex:Err ->
                 #ecomm_conn_udp{err_report = ErrReport} = S,
                 ecomm_fn:apply(ErrReport, [{Ex, Err, erlang:get_stacktrace(), AppState}])
         end)).

handle_data(PacketIn, #ecomm_conn_udp{listen_socket = LSock, peer = Peer, dec_state = DecState,
				      conn_stat = ConnStat, codec = Codec, app_handler = AppHandler,
                                      state = AppState, throttler = Throttler} = S) ->
    ?report_err(
       begin
           AppState1 = [{protocol, udp}, {socket, LSock}, {peer, Peer}, {conn_pid, self()} | AppState],
           case conn_start(ConnStat, AppState1) of
               {ok, AppState2} ->
                   S1 = S#ecomm_conn_udp{state = AppState2},
                   case decode(Codec, PacketIn, DecState) of
                       {ok, Request, ProcDict} ->
                           HandleReq =
                               fun () ->
                                       ?report_err(
                                          begin
                                              [put(K, V) || {K, V} <- ProcDict],
                                              case control(AppHandler, Request, Peer, AppState2) of
                                                  {reply, Reply, {TargetIP, TargetPort}} ->
                                                      handle_reply(Reply, {TargetIP, TargetPort}, S1),
                                                      {stop, normal, S1};
                                                  noreply ->
                                                      {stop, normal, S1};
                                                  {stop, Reply, {TargetIP, TargetPort}, Reason} ->
                                                      handle_reply(Reply, {TargetIP, TargetPort}, S1),
                                                      {stop, Reason, S1};
                                                  {stop, Reason} ->
                                                      {stop, Reason, S1}
                                              end
                                          end, AppState1, S)
                               end,
                           if Throttler == undefined -> HandleReq();
                              true -> run_throttled(HandleReq, Request, S)
                           end;
                       Error ->
                           {stop, Error, S}
                   end;
               {stop, Reason, AppState2} ->
                   {stop, Reason, S#ecomm_conn_udp{state = AppState2}}
           end
       end, AppState, S).


handle_reply(Reply, {TargetIP, TargetPort}, 
             #ecomm_conn_udp{listen_socket = LSock, codec = Codec, state = AppState} = S) ->
    ?report_err(
       case encode(Codec, Reply) of
           {ok, PacketOut} ->
               case send_data(LSock, {TargetIP, TargetPort}, PacketOut) of
                   ok ->
                       {stop, normal, S};
                   {stop, Reason} ->
                       {stop, Reason, S}
               end;
           Error ->
               {stop, Error, S}
       end, AppState, S).


run_throttled(Fun, Request, #ecomm_conn_udp{throttler = Throttler, peer = Peer} = S) ->
    case ecomm_throttle:run(Throttler, Fun) of
        overload ->
            handle_reply({overload, Request}, Peer, S);
        FunResult ->
            FunResult
    end.
        
%%%%%%%%%% no gen_server state here

conn_start(ConnStat, AppState) ->
    case ecomm_fn:apply(ConnStat, [{start, udp, AppState}]) of
        ok ->
            AppState;
	{ok, AppState1} when is_list(AppState1) ->
	    {ok, AppState1};
	{stop, Reason, AppState1} ->
	    {stop, Reason, AppState1}
    end.

-define(is_sync_flag(SyncAsync), (SyncAsync == sync orelse SyncAsync == async)).

decode(Codec, Packet, DecState) ->
    case ecomm_fn:apply(Codec, [{decode, Packet, DecState}]) of
	{ok, Term} ->
	    {ok, Term, []};
        {ok, Term, SyncAsync} when ?is_sync_flag(SyncAsync) ->
            %% ignoring sync/async, as UDP packet is already handled in separate process
            {ok, Term, []};
        {ok, Term, _DecState1} ->
	    {ok, Term, []};
        {ok, Term, _DecState1, SyncAsync} when ?is_sync_flag(SyncAsync) ->
            %% ignoring sync/async, as UDP packet is already handled in separate process
	    {ok, Term, []};
        {ok, Term, _DecState1, SyncAsync, ProcDict} when ?is_sync_flag(SyncAsync), is_list(ProcDict) ->
            %% ignoring sync/async, as UDP packet is already handled in separate process
	    {ok, Term, ProcDict};
	{incomplete, _DecState1} ->
	    {error, udp_incomplete};
	Error when element(1, Error) == error ->
	    Error
    end.

control(AppHandler, Request, Peer, AppState) ->
    case ecomm_fn:apply(AppHandler, [{Request, AppState}]) of
	ok ->
	    noreply;
	{ok, Reply} ->
	    {reply, Reply, Peer};
	{ok, Reply, OtherPeer} ->
	    {reply, Reply, OtherPeer};
	{stop, Reply, Reason} ->
	    {stop, Reply, Peer, Reason};
	{stop, Reply, OtherPeer, Reason} ->
	    {stop, Reply, OtherPeer, Reason};
	{stop, Reason} ->
	    {stop, Reason};
	stop ->
	    {stop, normal}
    end.

encode(Codec, Term) ->
    case ecomm_fn:apply(Codec, [{encode, Term}]) of
	{ok, Packet} when is_binary(Packet) ->
	    {ok, Packet};
	Error when element(1, Error) == error ->
	    Error
    end.

send_data(Socket, {TargetIP, TargetPort}, Packet) ->
    gen_udp:send(Socket, TargetIP, TargetPort, Packet).

conn_stop(ConnStat, Reason, AppState) ->
    ecomm_fn:apply(ConnStat, [{stop, udp, Reason, AppState}]).
