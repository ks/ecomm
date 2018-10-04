-module(ecomm_test).

-export([gen_load/4,
         format_udate_bin/0, format_date_bin/1,
         with_tcp/2, tcp_send/2, tcp_send/3, tcp_send_recv/2, tcp_send_recv/3,
         with_udp/1, udp_send/2, udp_send_recv/2, udp_send_recv/3,
         send/3, send_recv/3, send_recv/4]).

%%%==============================================
%%% Helpers for testing
%%%==============================================

gen_load(F, NumProcs, TestRunMS, ReqDelayMS)
  when is_function(F, 2),
       is_integer(TestRunMS), TestRunMS > 0,
       is_integer(NumProcs), NumProcs > 0,
       is_integer(ReqDelayMS), ReqDelayMS >= 0 ->
    Gen = fun (G, ProcId, ReqId) ->
                  case F(ProcId, ReqId) of
                      stop ->
                          bye;
                      _ ->
                          ReqDelayMS == 0 orelse timer:sleep(ReqDelayMS),
                          G(G, ProcId, ReqId + 1)
                  end
          end,
    spawn(fun () ->
                  Ref = make_ref(),
                  erlang:send_after(TestRunMS, self(), {stop, Ref}),
                  Pids = [spawn_link(fun () ->
                                             receive
                                                 {start, Ref} ->
                                                     Gen(Gen, ProcId, 0)
                                             end
                                     end) ||
                             ProcId <- lists:seq(1, NumProcs)],
                  [Pid ! {start, Ref} || Pid <- Pids],
                  receive
                      {stop, Ref} ->
                          [erlang:exit(Pid, kill) || Pid <- Pids],
                          bye
                  end
          end).

-define(is_timeout(Timeout), (Timeout == infinity orelse (is_integer(Timeout) andalso Timeout >= 0))).

tcp_connect({Host, Port}) ->
    gen_tcp:connect(Host, Port, [binary, {active, false}]).
tcp_connect({Host, Port}, Timeout) ->
    gen_tcp:connect(Host, Port, [binary, {active, false}], Timeout).
with_tcp({TgtHost, TgtPort}, SockFn) when is_function(SockFn, 2) ->
    case tcp_connect({TgtHost, TgtPort}) of
        {ok, Socket} ->
            try SockFn(Socket, infinity) after gen_tcp:close(Socket) end;
        Error ->
            Error
    end.
with_tcp({TgtHost, TgtPort}, SockFn, infinity) when is_function(SockFn, 2) ->
    with_tcp({TgtHost, TgtPort}, SockFn);
with_tcp({TgtHost, TgtPort}, SockFn, Timeout) when is_function(SockFn, 2), Timeout >= 0 ->
    case timer:tc(fun () -> tcp_connect({TgtHost, TgtPort}, Timeout) end) of
        {ElapsedUS, {ok, Socket}} ->
            case round(Timeout - (ElapsedUS / 1000)) of
                RemainingMS when RemainingMS >= 0 ->
                    try SockFn(Socket, RemainingMS) after gen_tcp:close(Socket) end;
                _ ->
                    {error, timeout}
            end;
        {_, Error} ->
            Error
    end.
tcp_send({Host, Port}, BinChunk) when is_binary(BinChunk) ->
    with_tcp({Host, Port}, fun (Socket, infinity) -> gen_tcp:send(Socket, BinChunk) end).
tcp_send({Host, Port}, BinChunk, Timeout) when is_binary(BinChunk) ->
    with_tcp({Host, Port}, fun (Socket, _RemMS) -> gen_tcp:send(Socket, BinChunk) end, Timeout).
tcp_send_recv({Host, Port}, BinChunk) when is_binary(BinChunk) ->
    tcp_send_recv({Host, Port}, BinChunk, infinity).
tcp_send_recv({Host, Port}, BinChunk, Timeout)
  when is_binary(BinChunk) andalso ?is_timeout(Timeout) ->
    with_tcp({Host, Port},
             fun (Socket, RemMS) ->
                     ok = gen_tcp:send(Socket, BinChunk),
                     gen_tcp:recv(Socket, 0, RemMS)
             end,
             Timeout).

udp_open() ->
    gen_udp:open(0, [binary, {active, false}]).
with_udp(SockFn) when is_function(SockFn, 1) ->
    case udp_open() of
        {ok, Socket} -> try SockFn(Socket) after gen_udp:close(Socket) end;
        Error -> Error
    end.
udp_send({Host, Port}, BinChunk) when is_binary(BinChunk) ->
    with_udp(fun (Socket) -> gen_udp:send(Socket, Host, Port, BinChunk) end).
udp_send_recv({Host, Port}, BinChunk) when is_binary(BinChunk) ->
    udp_send_recv({Host, Port}, BinChunk, infinity).
udp_send_recv({Host, Port}, BinChunk, Timeout)
  when is_binary(BinChunk) andalso ?is_timeout(Timeout) ->
    with_udp(fun (Socket) ->
                     ok = gen_udp:send(Socket, Host, Port, BinChunk),
                     gen_udp:recv(Socket, 0, Timeout)
             end).

send(udp, {Host, Port}, BinChunk) when is_binary(BinChunk) ->
    udp_send({Host, Port}, BinChunk);
send(tcp, {Host, Port}, BinChunk) when is_binary(BinChunk) ->
    tcp_send({Host, Port}, BinChunk).
send_recv(udp, {Host, Port}, BinChunk) when is_binary(BinChunk) ->
    udp_send_recv({Host, Port}, BinChunk, infinity);
send_recv(tcp, {Host, Port}, BinChunk) when is_binary(BinChunk) ->
    tcp_send_recv({Host, Port}, BinChunk, infinity).
send_recv(udp, {Host, Port}, BinChunk, Timeout) when is_binary(BinChunk), ?is_timeout(Timeout) ->
    udp_send_recv({Host, Port}, BinChunk, Timeout);
send_recv(tcp, {Host, Port}, BinChunk, Timeout) when is_binary(BinChunk), ?is_timeout(Timeout) ->
    tcp_send_recv({Host, Port}, BinChunk, Timeout).


format_udate_bin() ->
    format_date_bin(calendar:universal_time()).
format_date_bin({{_, _, _}, {_, _, _}} = DT) ->
    list_to_binary(httpd_util:rfc1123_date(DT));
format_date_bin({T1, T2, T3}) when is_integer(T1), is_integer(T2), is_integer(T3) ->
    list_to_binary(httpd_util:rfc1123_date(calendar:now_to_local_time({T1, T2, T3}))).
