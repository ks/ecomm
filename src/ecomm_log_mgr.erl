-module(ecomm_log_mgr).

-behaviour(gen_server).

-export([search/4, state/0, get_log/1]).

-export([start_link/1, 
         init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("ecomm_time.hrl").
-include_lib("kernel/include/file.hrl").

%%%%

-define(MAX_CACHED_LOG_AGE_SECS, 60).
-define(MF_PAT, {_, _}).
-define(is_modfun(MF), (is_tuple(MF) andalso size(MF) == 2 andalso 
                        is_atom(element(1, MF)) andalso is_atom(element(2, MF)))).

-record(log, {kind  :: atom(),
              tab   :: ets:table(),
              t1    :: os:timestamp(), % uni
              t2    :: os:timestamp(), % uni
              d1    :: calendar:datetime(), % loc
              d2    :: calendar:datetime(), % loc
              files :: [{string(), file:file_info()}],
              timer :: timer:timer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link({?MF_PAT = MF1, ?MF_PAT = MF2, ?MF_PAT = MF3} = KindFns)
  when ?is_modfun(MF1) andalso ?is_modfun(MF2) andalso ?is_modfun(MF3) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [KindFns], []).

search(Kind, {AnyAll, MsgKVREPatterns}, TMRange, MsgsBatchSize) ->
    Arg = {search, Kind, {AnyAll, MsgKVREPatterns}, TMRange, MsgsBatchSize},
    gen_server:call(?MODULE, Arg).

state() ->
    gen_server:call(?MODULE, state).

get_log(Kind) ->
    gen_server:call(?MODULE, {get_log, Kind}).
    
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([KindFns]) ->
    {ok, {KindFns, []}}.

handle_call(state, _From, ST) ->
    {reply, ST, ST};
handle_call({get_log, Kind}, _From, {_, Logs} = ST) ->
    {reply, get_log(Kind, Logs), ST};
handle_call({search, Kind, Filters, TMRange, MsgsBatchSize}, _, {{_, _, _}, _Logs} = ST) ->
    {ok, #log{tab = Tab}, ST1, _Counts} = sync_log(Kind, TMRange, ST),
    TMR = case TMRange of
              {UL, _} when UL == loc orelse UL == uni -> ecomm_time:tm(TMRange);
              ?TIMESTAMP_RANGE_WILDCARD -> TMRange;
              undefined -> {from, {0, 0, 0}}
          end,
    {reply, ecomm_log:search(Tab, Filters, TMR, MsgsBatchSize), ST1}.

handle_info({purge_log, Kind}, {KindFns, Logs}) ->
    Logs1 = purge_log(Kind, Logs),
    {noreply, {KindFns, Logs1}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _ST) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

sync_log(Kind, TMRange, {{FSelFn, FDefFn, MsgDefFn} = KindFns, Logs}) ->
    [FSel, FDef, MsgDef] = [M:F(Kind) || {M, F} <- [FSelFn, FDefFn, MsgDefFn]],
    SelectFn = case TMRange of
                   undefined -> fun () -> ecomm_file:select(FSel) end;
                   _ -> fun () -> ecomm_file:select_within(FSel, FDef, TMRange) end
               end,
    FFIsLen = fun ({A, U, D}) -> {length(A), length(U), length(D)} end,
    case get_log(Kind, Logs) of
        #log{tab = Tab, files = [_ | _] = LoadedFFIs} = L when Tab /= empty_none ->
            {AddFFIs, UpdFFIs, DelFFIs} = AllFFIs = ecomm_file:updates(SelectFn(), LoadedFFIs),
            Del = fun ({F, _}, FFIs) -> lists:keydelete(F, 1, FFIs) end,
            Upd = fun ({F, I}, FFIs) -> lists:keyreplace(F, 1, FFIs, {F, I}) end,
            Add = fun (FFI, FFIs) -> [FFI | FFIs] end,
            LoadTMRange = if TMRange == undefined -> fun (_) -> true end;
                             true -> {FDef, TMRange}
                          end,
            LoadedFFIs1 =
                lists:foldl(
                  fun ({FFIs, Fn}, Acc) ->
                          lists:foldl(
                            fun ({F, FI}, Acc1) ->
                                    {ok, Tab, _} = ecomm_log:load([F], MsgDef, LoadTMRange, Tab),
                                    Fn({F, FI}, Acc1)
                            end, Acc, FFIs)
                  end,
                  lists:foldl(Del, LoadedFFIs, DelFFIs),
                  [{UpdFFIs, Upd}, {AddFFIs, Add}]),
            {{T1, T2}, {D1, D2}} = ecomm_log:time_boundaries(Tab),
            L1 = L#log{t1 = T1, t2 = T2, d1 = D1, d2 = D2, files = LoadedFFIs1},
            {ok, L1, {KindFns, set_log(L1, Logs)}, FFIsLen(AllFFIs)};
        #log{tab = empty_none, files = LoadedFFIs} ->
            AllFFIs = ecomm_file:updates(SelectFn(), LoadedFFIs),
            OptTMRange = if TMRange == undefined -> []; true -> [TMRange] end,
            {ok, Log} = load_log(Kind, KindFns, OptTMRange),
            {ok, Log, {KindFns, set_log(Log, Logs)}, FFIsLen(AllFFIs)};
        false ->
            OptTMRange = if TMRange == undefined -> []; true -> [TMRange] end,
            {ok, #log{files = FFIs} = L} = load_log(Kind, KindFns, OptTMRange),
            {ok, L, {KindFns, [L | Logs]}, FFIsLen({FFIs, [], []})}
    end.
                     

get_log(Kind, Logs) ->
    lists:keyfind(Kind, #log.kind, Logs).

set_log(#log{kind = Kind} = L, Logs) ->
    lists:keyreplace(Kind, #log.kind, Logs, L).

del_log(#log{kind = Kind}, Logs) ->
    lists:keydelete(Kind, #log.kind, Logs).
            

load_log(Kind, KindFns, OptTimeRange) ->
    {ok, Tab, Filenames} = load_log_table(Kind, KindFns, OptTimeRange),
    {FFIs, []} = ecomm_file:file_infos(Filenames),
    {ok, Timer} = timer:send_after(timer:seconds(?MAX_CACHED_LOG_AGE_SECS), {purge_log, Kind}),
    case ets:info(Tab, size) of
        1 -> % schema
            {ok, #log{kind = Kind, tab = empty_none, files = FFIs}};
        _ ->
            {{T1, T2}, {D1, D2}} = ecomm_log:time_boundaries(Tab),
            {ok, #log{kind = Kind, tab = Tab,
                      t1 = T1, t2 = T2, d1 = D1, d2 = D2, 
                      timer = Timer, files = FFIs}}
    end.
load_log_table(Kind, {FSelFn, _, MsgDefFn}, []) ->
    [FSel, MsgDef] = [M:F(Kind) || {M, F} <- [FSelFn, MsgDefFn]],
    ecomm_log:load(FSel, MsgDef, fun (_) -> true end);
load_log_table(Kind, {FSelFn, FDefFn, MsgDefFn}, [Ds]) ->
    [FSel, FDef, MsgDef] = [M:F(Kind) || {M, F} <- [FSelFn, FDefFn, MsgDefFn]],
    ecomm_log:load(FSel, MsgDef, {FDef, Ds}).
                
purge_log(Kind, Logs) ->
    case get_log(Kind, Logs) of
        false -> 
            Logs;
        #log{tab = empty_none, timer = undefined} = L -> 
            del_log(L, Logs);
        #log{tab = Tab, timer = Timer} = L ->
            ets:delete(Tab),
            catch timer:cancel(Timer),
            del_log(L, Logs)
    end.
