-module(ecomm_slb).

-behaviour(gen_server).

%% (a very) Simple Load Balancer:
%% - no monitoring of elts 
%% - round-robin only

%% API
-export([start_link/0, start_link/1, stop/1]).
-export([insert/2, remove/2, get/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([t/0, t/4]).

%% internal use only
-record(ecomm_slb, 
        {elt_idx    :: pos_integer(),
         elts       :: tuple()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() -> 
    gen_server:start_link(?MODULE, [], []).
start_link(LB) when is_atom(LB) -> 
    gen_server:start_link({local, LB}, ?MODULE, [], []).
stop(LB) ->
    gen_server:stop(LB).

insert(LB, Elt) ->
    gen_server:call(LB, {insert, Elt}).
remove(LB, Elt) ->
    gen_server:call(LB, {remove, Elt}).
get(LB) ->
    gen_server:call(LB, get).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #ecomm_slb{elt_idx = 1, 
                    elts = {}}}.

handle_call({insert, Elt}, _From, #ecomm_slb{elts = Elts} = S) ->
    {reply, Elt, S#ecomm_slb{elts = add_elt(Elt, Elts)}};
handle_call({remove, Elt}, _From, #ecomm_slb{elts = Elts} = S) ->
    {reply, ok, S#ecomm_slb{elts = delete_elt(Elt, Elts)}};
handle_call(get, _From, S) ->
    case do_schedule(S) of
        {ok, Elt, S1} ->
            {reply, {ok, Elt}, S1};
        {error, empty} ->
            {reply, {error, empty}, S}
    end.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_schedule(#ecomm_slb{elts = {}}) ->
    {error, empty};
do_schedule(#ecomm_slb{elt_idx = Idx, elts = Elts} = S) ->
    {ok, element(Idx, Elts), S#ecomm_slb{elt_idx = next_idx(Idx, size(Elts))}}.
    
next_idx(Idx, EltsSize) when Idx < EltsSize ->
    Idx + 1;
next_idx(Idx, EltsSize) when Idx >= EltsSize ->
    1.

add_elt(Elt, Elts) when is_tuple(Elts) ->
    EltsList = tuple_to_list(Elts),
    case lists:member(Elt, EltsList) of
        false -> list_to_tuple([Elt | EltsList]);
        true -> Elts
    end.
delete_elt(Elt, Elts) when is_tuple(Elts) ->
    list_to_tuple(tuple_to_list(Elts) -- [Elt]).

%%%%%%%%%%

t(Name, NumElts, NumProcs, ProcSampleSize) ->
    {ok, SLB} = start_link(Name),
    Get = fun (Ident) ->
                  Res = [begin {ok, I} = ecomm_slb:get(SLB), timer:sleep(1), I end ||
                            _ <- lists:seq(1, ProcSampleSize)],
                  {Ident, Res}
          end,
    [insert(SLB, I) || I <- lists:seq(1, NumElts)],
    TopRecv = self(),
    TopRef = make_ref(),
    ProcIds = lists:seq(1, NumProcs),
    [spawn(fun () -> TopRecv ! {TopRef, Get(ProcId)} end) || ProcId <- ProcIds],
    [receive 
         {Ref, {ProcId, Res}} when Ref == TopRef ->
             io:format("!!!!!!!!!! ~p~n", [{ProcId, Res}])
     end || _ <- ProcIds],
    SLB.

t() ->
    t(slb_test, 10, 10, 35).


