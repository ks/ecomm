-module(ecomm_heart).

-behaviour(gen_server).

%%%% Simple heartbeat

-export([start_link/4, start_link/5, is_beating/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% internal
-export([worker/3, caller/3, notifier/2]).
-export([t/1]).

-record(ecomm_heartbeat_state,
        {name        :: atom(),
         mfa         :: {atom(), atom(), list()},
         worker      :: undefined | pid(),
         timeout     :: integer(),
         beating     :: true | false | crash,
         subscribers :: [{atom() | pid(), list()}]}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(MFA, IntervalSecs, TimeoutSecs, Subscribers) ->
    start_link(undefined, MFA, IntervalSecs, TimeoutSecs, Subscribers).
start_link(Name, {M, F, A} = MFA, IntervalSecs, TimeoutSecs, Subscribers) 
  when IntervalSecs > TimeoutSecs, is_atom(M), is_atom(F), is_list(A), is_list(Subscribers) ->
    {module, _} = code:ensure_loaded(M),
    true = erlang:function_exported(M, F, length(A)),
    NameArg = case Name of
                  undefined -> [];
                  {_, LocName} when is_atom(LocName) -> [Name];
                  LocName when is_atom(LocName) -> [{local, LocName}]
              end,
    StartArgs = NameArg ++ [?MODULE, [Name, MFA, IntervalSecs, TimeoutSecs, Subscribers], []],
    apply(gen_server, start_link, StartArgs).

is_beating(HB) ->
    gen_server:call(HB, is_beating).

stop(HB) ->
    gen_server:cast(HB, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name, {M, F, Args}, IntervalSecs, TimeoutSecs, Subscribers]) ->
    Subscribers1 = [case Subscriber of
                        {Tgt, KVs} when is_pid(Tgt), is_list(KVs) -> {Tgt, KVs};
                        {Tgt, KVs} when is_atom(Tgt), is_list(KVs) -> {Tgt, KVs};
                        Tgt when is_atom(Tgt); is_pid(Tgt) -> {Tgt, []}
                    end || Subscriber <- Subscribers],
    timer:send_interval(timer:seconds(IntervalSecs), heartbeat),
    erlang:send(self(), heartbeat),
    {ok, #ecomm_heartbeat_state{name = Name,
                                mfa = {M, F, Args},
                                worker = undefined,
                                timeout = timer:seconds(TimeoutSecs),
                                beating = undefined,
                                subscribers = Subscribers1}}.

handle_info(heartbeat, #ecomm_heartbeat_state{worker = undefined, mfa = MFA, timeout = TM} = S) ->
    Worker = spawn_link(?MODULE, worker, [self(), MFA, TM]),
    {noreply, S#ecomm_heartbeat_state{worker = Worker}}.

handle_cast({beating, Beating}, #ecomm_heartbeat_state{beating = Beating} = S) ->
    {noreply, S#ecomm_heartbeat_state{beating = Beating, worker = undefined}};
handle_cast({beating, Beating}, #ecomm_heartbeat_state{mfa = MFA, 
                                                       name = Name, 
                                                       subscribers = Subscribers} = S) ->
    FirstKVs = [{is_beating, Beating}, {name, Name}, {mfa, MFA}, {heart_pid, self()}],
    spawn_link(?MODULE, notifier, [FirstKVs, Subscribers]),
    {noreply, S#ecomm_heartbeat_state{beating = Beating, worker = undefined}};
handle_cast(stop, S) ->
    {stop, normal, S}.
handle_call(is_beating, _From, #ecomm_heartbeat_state{beating = Beating} = S) ->
    {reply, Beating, S}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

worker(HB, {M, F, Args}, TimeoutMS) ->
    Wrk = self(),
    Ref = make_ref(),
    Caller = spawn_link(?MODULE, caller, [Wrk, Ref, {M, F, Args}]),
    Beating = receive {Ref, Beat} -> 
                      Beat
              after TimeoutMS ->
                      erlang:exit(Caller, kill), % can hang there
                      false
              end,
    gen_server:cast(HB, {beating, Beating}).

caller(Wrk, Ref, {M, F, Args}) ->
    Wrk ! {Ref, case catch apply(M, F, Args) of
                    {'EXIT', _} -> crash;
                    true -> true;
                    _ -> false
                end}.

notifier(FirstKVs, Subscribers) ->
    [Tgt ! {ecomm_heart, (FirstKVs ++ LastKVs)} || {Tgt, LastKVs} <- Subscribers].


%%%%%%%%%%

t(Subs) ->
    ecomm_heart:start_link({timer, sleep, [10000]}, 5, 3, Subs).
