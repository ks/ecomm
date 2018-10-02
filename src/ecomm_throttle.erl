-module(ecomm_throttle).

-behaviour(gen_server).

-export([start/3, name/1, run/2]). %% API

-export([kill/1, test/2]). % testing & development

-export([start_link/3]). %% internal API 
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]). % callbacks

%% internal use only
-record(ecomm_throttle,
        {queue          :: ets:table(),
         max_queue_len  :: integer(),
         last_sent      :: undefined | erlang:timestamp(),
         min_diff_us    :: integer(),
         max_req_age_ms :: integer(),
         timer          :: undefined | {once | interval, timer:tref()}}).

%%%===================================================================
%%% API
%%%===================================================================

start(Name, MaxRequestsPerSec, MaxRequestAgeMSec)
  when is_atom(Name), is_integer(MaxRequestsPerSec), is_integer(MaxRequestAgeMSec) ->
    {ok, Sup} = ecomm_throttle_sup:start_link(Name, MaxRequestsPerSec, MaxRequestAgeMSec),
    [{_, ThrottlerPid, _, _}] = supervisor:which_children(Sup),
    {ok, ThrottlerPid}.

name(Name) when is_atom(Name) ->
    binary_to_atom(<<"ecomm_throttle_", (atom_to_binary(Name, utf8))/binary>>, utf8);
name(Id) when is_integer(Id) ->
    binary_to_atom(<<"ecomm_throttle_", (integer_to_binary(Id))/binary>>, utf8).

run(Throttler, Fun) when is_function(Fun, 0) ->
    do_run(Throttler, Fun).

%%%===================================================================
%%% INTERNAL API & TESTING & DEVELOPMENT
%%%===================================================================

%% called by supervisor
start_link(Queue, MaxRequestsPerSec, MaxRequestAgeMSec) ->
    gen_server:start_link({local, Queue}, ?MODULE, [Queue, MaxRequestsPerSec, MaxRequestAgeMSec], []).

kill(Name) ->
    exit(whereis(ecomm_throttle_sup:name(Name)), kill).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Queue, MaxRequestsPerSec, MaxRequestAgeMSec]) ->
    MinDiffUS = round((1 / MaxRequestsPerSec) * 1000000),
    MaxQueueLen = round(MaxRequestAgeMSec / (MinDiffUS / 1000)),
    {ok, #ecomm_throttle{queue = Queue,
                         max_queue_len = MaxQueueLen,
                         min_diff_us = MinDiffUS,
                         max_req_age_ms = MaxRequestAgeMSec,
                         timer = case ets:info(Queue, size) > 0 of % failed throttler restarted
                                     true ->
                                         {ok, TRef} = timer:send_after(0, pull_queue),
                                         {once, TRef};
                                     false ->
                                         undefined
                                 end}}.

handle_info(pull_queue, #ecomm_throttle{timer = undefined} = S) -> % leftovers from cancelled timer
    {noreply, S};
handle_info(pull_queue, #ecomm_throttle{queue = Queue,
                                        min_diff_us = MinDiffUS,
                                        timer = {Kind, TRef} = Timer} = S) ->
    case ets:first(Queue) of
        '$end_of_table' ->
            if Kind == interval -> timer:cancel(TRef); true -> nop end,
            {noreply, S#ecomm_throttle{timer = undefined}};
        Timestamp ->
            [{_, {{Caller, Ref}, Fun}}] = ets:lookup(Queue, Timestamp),
            Now = os:timestamp(),
            spawn_send_fun_result({run, {Caller, Ref}, Fun}),
            ets:delete(Queue, Timestamp),
            HasMore = ets:info(Queue, size) > 0,
            Timer1 = case {Kind, HasMore} of
                         {once, true} ->
                             {ok, TRef1} = timer:send_interval(trunc(MinDiffUS / 1000), pull_queue),
                             {interval, TRef1};
                         {once, false} ->
                             undefined;
                         {interval, true} ->
                             Timer;
                         {interval, false} ->
                             timer:cancel(TRef),
                             undefined
                     end,
            {noreply, S#ecomm_throttle{last_sent = Now, timer = Timer1}}
    end;

handle_info({run, _, _} = RunReq, #ecomm_throttle{last_sent = undefined} = S) ->
    spawn_send_fun_result(RunReq),
    {noreply, S#ecomm_throttle{last_sent = os:timestamp()}};

handle_info({run, {Caller, Ref}, Fun} = RunReq, #ecomm_throttle{queue = Queue,
                                                                max_queue_len = MaxQueueLen,
                                                                last_sent = LastSent,
                                                                min_diff_us = MinDiffUS,
                                                                timer = Timer} = S) ->
    Now = os:timestamp(),
    LastSentDiffUS = timer:now_diff(Now, LastSent),
    case LastSentDiffUS >= MinDiffUS of
        true ->
            spawn_send_fun_result(RunReq),
            {noreply, S#ecomm_throttle{last_sent = Now}};
        false ->
            case ets:info(Queue, size) < MaxQueueLen of
                true ->
                    ets:insert(Queue, {Now, {{Caller, Ref}, Fun}}),
                    case Timer of
                        undefined ->
                            NextSendDiffMS = round((MinDiffUS - LastSentDiffUS) / 1000),
                            {ok, TRef} = timer:send_after(NextSendDiffMS, pull_queue),
                            {noreply, S#ecomm_throttle{timer = {once, TRef}}};
                        {_, _} ->
                            {noreply, S}
                    end;
                false ->
                    send_result({run, {Caller, Ref}, overload}),
                    {noreply, S}
            end
    end.

handle_call(_Req, _From, S) -> {reply, nop, S}.
handle_cast(_Msg, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

spawn_send_fun_result({run, {Caller, Ref}, Fun}) ->
    spawn(fun () -> Caller ! {throttler_res, Ref, catch Fun()} end).

send_result({run, {Caller, Ref}, Result}) ->
    Caller ! {throttler_res, Ref, Result}.

do_run(Throttler, Fun) ->
    Caller = self(),
    Ref = make_ref(),
    Throttler ! {run, {Caller, Ref}, Fun},
    receive
        {throttler_res, Ref, Res} ->
            Res
    end.

%%%===================================================================
%%% TEST
%%%===================================================================

test(Name, ReqsCount) ->
    Throttler = name(Name),
    [spawn(fun () ->
                   Start = os:timestamp(),
                   Res = run(Throttler,
                             fun () ->
                                     Stop = os:timestamp(),
                                     DiffMS = timer:now_diff(Stop, Start) / 1000,
                                     io:format(">>> running ~p (~p MS)~n", [X, DiffMS])
                             end),
                   io:format("<<< result (~p) = ~p~n", [X, Res])
           end) || X <- lists:seq(1, ReqsCount)].
