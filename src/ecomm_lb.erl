-module(ecomm_lb).
%% load balancer handling processes only, for simple load balancer, there's ecomm_slb
-behaviour(gen_server).

-export([start_link/4, start_link/5, start_link/6]).
-export([request/3, request/4]).
-export([replace_failed_elt/2, elts_list/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% internal
-export([start_elt/2, try_request/5]).
-export([t/0]).

-define(RETRY_START_MS, 10000).

%% internal use only
-record(ecomm_lb,
        {ident       :: atom(),
         start_elt   :: fun() | {atom(), atom()},
         stop_elt    :: fun() | {atom(), atom()},
         report_elt  :: undefined | fun () | {atom(), atom()},
         avail       :: {Min :: integer(), Max :: integer()},
         reply_tm_ms :: infinity | non_neg_integer(),
         start_pid   :: undefined | pid(),
         elt_idx     :: pos_integer(),
         elts        :: tuple()}).

%%%===================================================================
%%% API
%%%===================================================================

-define(is_fun_spec(F, Arity), (is_function(F, Arity) orelse (is_tuple(F) andalso size(F) == 2))).

start_link(Name, Avail, StartElt, StopElt) ->
    start_link(Name, Avail, StartElt, StopElt, undefined).
start_link(Name, Avail, StartElt, StopElt, ReportElt) ->
    start_link(Name, Avail, StartElt, StopElt, ReportElt, infinity).
start_link(Name, {Min, Max} = Avail, StartElt, StopElt, ReportElt, ReplyTimeoutMSecs)
  when is_atom(Name) andalso
       ?is_fun_spec(StartElt, 0) andalso
       (ReportElt == undefined orelse ?is_fun_spec(ReportElt, 1)) andalso
       (Min =< Max andalso Min > 0) andalso
       (StopElt == undefined orelse ?is_fun_spec(StopElt, 1)) andalso
       (ReplyTimeoutMSecs == infinity 
        orelse (is_integer(ReplyTimeoutMSecs) andalso ReplyTimeoutMSecs >= 0)) ->
    ecomm_fn:ensure(StartElt, 0),
    if ReportElt /= undefined -> ecomm_fn:ensure(ReportElt, 1); true -> ok end,
    if StopElt /= undefined -> ecomm_fn:ensure(StopElt, 1); true -> ok end,
    Args = [Name, Avail, StartElt, StopElt, ReportElt, ReplyTimeoutMSecs],
    gen_server:start_link({local, Name}, ?MODULE, Args, []).

request(LB, Fn, Data) ->
    request(LB, Fn, Data, undefined).
request(LB, Fn, Data, TimeoutMS) %% we can override reply timeout for particular request
  when is_function(Fn, 2) andalso 
       (TimeoutMS == undefined orelse TimeoutMS == infinity 
        orelse (is_integer(TimeoutMS) andalso TimeoutMS >= 0)) ->
    gen_server:call(LB, {request, Fn, Data, TimeoutMS}, infinity).

replace_failed_elt(LB, Elt) ->
    gen_server:cast(LB, {replace_failed_elt, Elt}).

elts_list(LB) ->
    gen_server:call(LB, elts_list).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Ident, Avail, StartElt, StopElt, ReportElt, ReplyTimeoutMSecs]) ->
    process_flag(trap_exit, true), %% we want to shut down managed workers
    cast_start_elt(),
    {ok, #ecomm_lb{ident = Ident, 
                   start_elt = StartElt, 
                   stop_elt = StopElt, 
                   report_elt = ReportElt,
                   avail = Avail,
                   reply_tm_ms = ReplyTimeoutMSecs,
                   elt_idx = 1,
                   elts = {}}}.

handle_info({'DOWN', _MRef, process, Pid, _Info}, #ecomm_lb{start_pid = Pid} = S) ->
    cast_start_elt(),
    {noreply, S#ecomm_lb{start_pid = undefined}};
handle_info({'DOWN', MRef, process, Pid, _Info}, #ecomm_lb{} = S) when is_pid(Pid) ->
    #ecomm_lb{ident = Ident, avail = Avail, elts = Elts} = S,
    Elts1 = delete_elt({Pid, MRef}, Elts),
    maybe_report(S#ecomm_lb.report_elt, {elt_deleted, Ident, Pid, size(Elts1), Avail}),
    cast_start_elt(),
    {noreply, S#ecomm_lb{elt_idx = 1, elts = Elts1}};
handle_info(retry_start_elt, S) ->
    cast_start_elt(),
    {noreply, S};
handle_info({'EXIT', _From, Reason}, S) ->
    {stop, Reason, S}.

handle_cast(start_elt, #ecomm_lb{start_pid = undefined, avail = {_, Max}, elts = Elts} = S)
  when size(Elts) < Max ->
    {Pid, _} = spawn_monitor(?MODULE, start_elt, [self(), S#ecomm_lb.start_elt]),
    {noreply, S#ecomm_lb{start_pid = Pid}};
handle_cast(start_elt, #ecomm_lb{} = S) ->
    {noreply, S};

handle_cast({add_elt, Elt}, #ecomm_lb{ident = Ident, avail = Avail, elts = Elts} = S) ->
    Mon = monitor(process, Elt),
    Elts1 = add_elt({Elt, Mon}, Elts),
    maybe_report(S#ecomm_lb.report_elt, {elt_added, Ident, Elt, size(Elts1), Avail}),
    {noreply, S#ecomm_lb{elts = Elts1}};
handle_cast({elt_start_failed, Error}, #ecomm_lb{ident = Ident, avail = Avail, elts = Elts} = S) ->
    maybe_report(S#ecomm_lb.report_elt, {elt_start_failed, Ident, Error, size(Elts), Avail}),
    {noreply, S#ecomm_lb{}};
handle_cast({replace_failed_elt, Elt}, #ecomm_lb{ident = Ident, avail = Avail, elts = Elts} = S) ->
    case find_elt_mon(Elt, Elts) of
        {ok, Mon} ->
            maybe_report(S#ecomm_lb.report_elt, {elt_deleted, Ident, Elt, size(Elts), Avail}),
            case is_process_alive(Elt) of
                true ->
                    demonitor(Mon),
                    catch (S#ecomm_lb.stop_elt)(Elt);
                false ->
                    nop
            end,
            cast_start_elt(),
            {noreply, S#ecomm_lb{elt_idx = 1, elts = delete_elt({Elt, Mon}, Elts)}};
        false ->
            {noreply, S}
    end.

handle_call(elts_list, _From, #ecomm_lb{elts = Elts} = S) ->
    {Pids, _Mons} = lists:unzip(tuple_to_list(Elts)),
    {reply, Pids, S};

handle_call({request, Fn, Data, OverrideTimeoutMS}, From, 
            #ecomm_lb{reply_tm_ms = ReplyTimeoutMS} = S) ->
    TimeoutMS = if OverrideTimeoutMS == undefined -> ReplyTimeoutMS; true -> OverrideTimeoutMS end,
    case do_schedule(S) of
        {ok, Elt, S1} ->
            spawn(?MODULE, try_request, [From, Fn, Elt, Data, TimeoutMS]),
            {noreply, S1};
        {error, min_limit} ->
            {reply, {error, min_limit}, S}
    end.

terminate(_Reason, #ecomm_lb{stop_elt = StopElt, elts = Elts}) ->
    foreach_elt(fun (Elt) -> catch StopElt(Elt) end, Elts).

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

cast_start_elt() ->
    gen_server:cast(self(), start_elt).

start_elt(LB, StartElt) ->
    Res = case catch ecomm_fn:apply(StartElt, []) of
              {ok, Elt} -> {add_elt, Elt};
              Error -> {elt_start_failed, Error}
          end,
    gen_server:cast(LB, Res).

do_schedule(#ecomm_lb{avail = {Min, _}, elt_idx = Idx, elts = Elts} = S) when Min =< size(Elts) ->
    {ok, element(1, element(Idx, Elts)), S#ecomm_lb{elt_idx = next_idx(Idx, size(Elts))}};
do_schedule(#ecomm_lb{avail = {Min, _}, elts = Elts}) when Min > size(Elts) ->
    {error, min_limit}.
    
next_idx(Idx, EltsSize) when Idx < EltsSize ->
    Idx + 1;
next_idx(Idx, EltsSize) when Idx >= EltsSize ->
    1.

try_request(From, Fn, Elt, Data, TimeoutMS) ->
    Reply = ecomm_fn:apply_tm(Fn, [Elt, Data], TimeoutMS),
    gen_server:reply(From, Reply).
            
maybe_report(undefined, _) -> 
    ok;
maybe_report(Reporter, Event) ->
    catch ecomm_fn:apply(Reporter, [Event]).

%%%%%%%%%%

add_elt({Pid, Mon}, Pids) when is_tuple(Pids) ->
    list_to_tuple([{Pid, Mon} | tuple_to_list(Pids)]).
delete_elt({Pid, Mon}, Pids) when is_tuple(Pids) ->
    list_to_tuple(tuple_to_list(Pids) -- [{Pid, Mon}]).

find_elt_mon(Pid, Pids) when is_pid(Pid) ->
    case lists:keyfind(Pid, 1, tuple_to_list(Pids)) of
        {Pid, Mon} -> {ok, Mon};
        false -> false
    end.

foreach_elt(Fn, Pids) when is_tuple(Pids) ->
    foreach_elt(Fn, Pids, 1, size(Pids)).
foreach_elt(_Fn, _Pids, I, S) when I > S ->
    ok;
foreach_elt(Fn, Pids, I, S) ->
    Fn(element(1, element(I, Pids))),
    foreach_elt(Fn, Pids, I + 1, S).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

t() ->
    Loop = fun (L) -> timer:sleep(100000), io:format("worker ~p loop~n", [self()]) , L(L) end,
    StartElt = fun () ->
                       Pid = spawn(fun () -> Loop(Loop) end),
                       io:format("////////// (test) StartElt ==> STARTED NEW ~p~n", [Pid]),
                       {ok, Pid}
               end,
    StopElt  = fun (Pid) -> erlang:exit(Pid, kill) end,
    Reporter = fun ({_Event, _Ident, _MaybeElt, _Count, {_Min, _Max}} = LBMsg) ->
                       io:format("////////// (test) Reporter: ~p~n", [LBMsg])
               end,
    
    {ok, LB} = start_link(test, {3, 10}, StartElt, StopElt, Reporter),
        
    ExecReq  = fun (Pid, Data) ->
                       io:format("////////// CALLING ~p with ~p~n", [Pid, Data]),
                       {ok, {data_res, Data}}
               end,

    timer:sleep(1000), % let start at least 3 

    Res = request(LB, ExecReq, xxx_data),

    io:format("////////// SYNCREQ RES = ~p~n", [Res]),

    {ok, LB}.


