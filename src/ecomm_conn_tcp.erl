%%%===================================================================
%%% This module is NOT to be used directly, but via:
%%%
%%% ecomm_conn_mgr:enable/1
%%% ecomm_conn_mgr:disable/1
%%%===================================================================
-module(ecomm_conn_tcp).

-behaviour(gen_server).

%% Internal API
-export([ensure_options/1, start/2, start/3, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]). % callbacks

%% internal use only
-record(ecomm_conn_tcp,
	{listen_socket :: gen_tcp:socket(),
	 socket        :: gen_tcp:socket(),
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

stop(Pid) ->
    gen_server:cast(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([{ConnStat, Codec, AppHandler, ErrReport}, AppState]) ->
    {ok, #ecomm_conn_tcp{dec_state = ecomm_fn:apply(Codec, [{decode, init}]),
                         conn_stat = ConnStat,
			 codec = Codec,
			 app_handler = AppHandler,
                         err_report = ErrReport,
			 state = AppState}};
init([{ConnStat, Codec, AppHandler, ErrReport}, AppState, Throttler]) ->
    {ok, S} = init([{ConnStat, Codec, AppHandler, ErrReport}, AppState]),
    {ok, S#ecomm_conn_tcp{throttler = Throttler}}.

handle_info({tcp_opened, {LSock, CSock}}, #ecomm_conn_tcp{socket = undefined} = S) ->
    handle_open(S#ecomm_conn_tcp{listen_socket = LSock, socket = CSock});

handle_info({tcp, _CSock, Packet}, #ecomm_conn_tcp{socket = CSock} = S) when CSock /= undefined ->
    handle_data(Packet, S);
handle_info({tcp_closed, _CSock}, S) ->
    {stop, normal, S}.

handle_cast({stop, Reason, State}, _) ->
    {stop, Reason, State};
handle_cast(stop, State) ->
    {stop, normal, State}.

terminate(Reason, #ecomm_conn_tcp{socket = Socket, conn_stat = ConnStat, state = AppState}) ->
    catch gen_tcp:close(Socket),
    conn_stop(ConnStat, Reason, AppState).

handle_call(_Request, _From, State) -> {reply, ignored, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


%%%==============================================
%%% TCP transport handling
%%%==============================================

ensure_options({listen, Opts}) ->
    BadKeys = gb_sets:from_list([list, binary, fd, tcp_module, active, deliver, exit_on_close, mode]),
    OptsSet = gb_sets:from_list(proplists:get_keys(Opts)),
    case gb_sets:to_list(gb_sets:intersection(OptsSet, BadKeys)) of
	[] -> {ok, [{active, true}, binary | Opts]};
	_ -> {error, bad_options}
    end.

%%%%%%%%%% result of handle_* functions is result of gen_server:handle_info function

-define(report_err(Body, AppState, S),
        (try Body of
             R -> R
         catch
             Ex:Err ->
                 #ecomm_conn_tcp{err_report = ErrReport} = S,
                 ecomm_fn:apply(ErrReport, [{Ex, Err, erlang:get_stacktrace(), AppState}])
         end)).

handle_open(#ecomm_conn_tcp{listen_socket = LSock, socket = CSock,
			    conn_stat = ConnStat, state = AppState} = S) ->
    AppState1 = [{protocol, tcp}, {listen_socket, LSock}, {socket, CSock}, {conn_pid, self()},
                 {peer, catch case inet:peername(CSock) of
                                  {ok, P} -> P;
                                  {error, E} -> E
                              end} | AppState],
    ?report_err(
       case conn_start(ConnStat, AppState1) of
           {ok, AppState2} ->
               {noreply, S#ecomm_conn_tcp{state = AppState2}};
           {stop, Reason, AppState2} ->
               {stop, Reason, S#ecomm_conn_tcp{state = AppState2}}
       end, AppState1, S).

handle_data(PacketIn, #ecomm_conn_tcp{socket = CSock, dec_state = DecState,
				      codec = Codec, app_handler = AppHandler,
				      state = AppState, throttler = Throttler} = S) ->
    ?report_err(
       case decode(Codec, PacketIn, DecState) of
           {ok, Request, DecState1, SyncAsync, ProcDict} ->
               S1 = S#ecomm_conn_tcp{dec_state = DecState1},
               if SyncAsync == sync andalso Throttler == undefined ->
                       [put(K, V) || {K, V} <- ProcDict],
                       handle_request(Request, CSock, {AppHandler, Codec}, AppState, S1);
                  SyncAsync == async ->
                       Conn = self(),
                       HandleReq =
                           fun () ->
                                   ?report_err(
                                      begin
                                          [put(K, V) || {K, V} <- ProcDict],
                                          case handle_request(Request, CSock, {AppHandler, Codec},
                                                              AppState, S1) of
                                              {noreply, _} ->
                                                  ok;
                                              {stop, Reason, S1} ->
                                                  gen_server:cast(Conn, {stop, Reason, S1})
                                          end
                                      end, AppState, S1)
                           end,
                       spawn(if Throttler == undefined -> HandleReq;
                                true -> throttled_thunk(HandleReq, Request, Conn, S)
                             end),
                       {noreply, S1}
               end;
           {incomplete, DecState1} ->
               {noreply, S#ecomm_conn_tcp{dec_state = DecState1}};
           Error ->
               {stop, Error, S}
       end, AppState, S).

handle_request(Request, CSock, {AppHandler, Codec}, AppState, S) ->
    ?report_err(
       case control(AppHandler, Request, AppState) of
           {reply, Reply} ->
               handle_reply(Reply, CSock, Codec, S);
           noreply ->
               {noreply, S};
           {stop, Reply, Reason} ->
               handle_reply(Reply, CSock, Codec, S),
               {stop, Reason, S};
           {stop, Reason} ->
               {stop, Reason, S}
       end, AppState, S).

handle_reply(Reply, CSock, Codec, #ecomm_conn_tcp{state = AppState} = S) ->
    ?report_err(
       case encode(Codec, Reply) of
           {ok, PacketOut} ->
               case send_data(CSock, PacketOut) of
                   ok ->
                       {noreply, S};
                   {stop, Reason} ->
                       {stop, Reason, S}
               end;
           Error ->
               {stop, Error, S}
       end, AppState, S).

throttled_thunk(Fun, Request, Conn, #ecomm_conn_tcp{throttler = Throttler,
                                                    socket = CSock,
                                                    codec = Codec} = S) ->
    fun () ->
            case ecomm_throttle:run(Throttler, Fun) of
                overload ->
                    case handle_reply({overload, Request}, CSock, Codec, S) of
                        {noreply, _} ->
                            nop;
                        {stop, Reason, _} ->
                            gen_server:cast(Conn, {stop, Reason, S})
                    end;
                FunResult ->
                    FunResult
            end
    end.


%%%%%%%%%% no gen_server state here

conn_start(ConnStat, AppState) ->
    case ecomm_fn:apply(ConnStat, [{start, tcp, AppState}]) of
	ok ->
            {ok, AppState};
	{ok, AppState1} when is_list(AppState1) ->
	    {ok, AppState1};
	{stop, Reason, AppState1} ->
	    {stop, Reason, AppState1}
    end.

-define(is_sync_flag(SyncAsync), (SyncAsync == sync orelse SyncAsync == async)).

decode(Codec, Packet, DecState) ->
    case ecomm_fn:apply(Codec, [{decode, Packet, DecState}]) of
	{ok, Term} ->
	    {ok, Term, ecomm_fn:apply(Codec, [{decode, init}]), async, []};
	{ok, Term, SyncAsync} when ?is_sync_flag(SyncAsync) ->
	    {ok, Term, ecomm_fn:apply(Codec, [{decode, init}]), SyncAsync, []};
	{ok, Term, DecState1} ->
	    {ok, Term, DecState1, async, []};
	{ok, Term, DecState1, SyncAsync} when ?is_sync_flag(SyncAsync) ->
	    {ok, Term, DecState1, SyncAsync, []};
	{ok, Term, DecState1, SyncAsync, ProcDict} when ?is_sync_flag(SyncAsync), is_list(ProcDict) ->
	    {ok, Term, DecState1, SyncAsync, ProcDict};
	{incomplete, DecState1} ->
	    {incomplete, DecState1};
	Error when element(1, Error) == error ->
	    Error
    end.

control(AppHandler, Request, AppState) ->
    case ecomm_fn:apply(AppHandler, [{Request, AppState}]) of
	ok ->
	    noreply;
	{ok, Reply} ->
	    {reply, Reply};
	{stop, Reply, Reason} ->
	    {stop, Reply,Reason};
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

send_data(Socket, Packet) ->
    case gen_tcp:send(Socket, Packet) of
	ok ->
	    ok;
	{error, closed} ->
	    {stop, normal};
	{error, Reason} ->
	    {stop, {error, Reason}}
    end.

conn_stop(ConnStat, Reason, AppState) ->
    ecomm_fn:apply(ConnStat, [{stop, tcp, Reason, AppState}]).
