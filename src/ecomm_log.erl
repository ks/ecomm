-module(ecomm_log).

-export([load/3, load/4, loadn/4, load1/4, load1/5, load1/6]).
-export([search/4, cont_timebases/3]).
-export([first_tm/1, last_tm/1, time_boundaries/1]).
-export([parse_file/2]). %% for testing only

-include("ecomm_time.hrl").

%%%%%%%%%%

load(FilenameSel, MsgFrameDef, WithinOrSpec) ->
    Tab = ets:new(undefined, [public, ordered_set, {write_concurrency, true}]),
    load(FilenameSel, MsgFrameDef, WithinOrSpec, Tab).

load(FilenameSel, MsgFrameDef, {FilenameFrameDef, {Mark, Ds}}, Tab) ->
    ULDS = ecomm_time:uni_loc_diff_secs(),
    Ts1 = ecomm_time:spec_to_tmrange({Mark, Ds}, ULDS),
    MsgWithin = fun (KVs) ->
                        MsgTM = ecomm_pl:getv(timestamp, KVs),
                        ecomm_time:within(MsgTM, Ts1)
                end,
    loadn(ecomm_file:select_within(FilenameSel, FilenameFrameDef, Ts1), MsgFrameDef, MsgWithin, Tab);
load(FilenameSel, MsgFrameDef, MsgWithin, Tab) when is_function(MsgWithin, 1) ->
    loadn(ecomm_file:select(FilenameSel), MsgFrameDef, MsgWithin, Tab).
    

loadn(Filenames, MsgFrameDef, MsgWithin, Tab) when is_function(MsgWithin, 1) ->
    ok = validate_msg_frame_def(MsgFrameDef),
    Loader = self(),
    Ref = make_ref(),
    MsgKeys = lists:keydelete(timestamp, 1, ecomm_scan:framedef_keys(MsgFrameDef)),
    [spawn(fun () -> 
                   load1(F, MsgFrameDef, MsgWithin, MsgKeys, Tab), 
                   Loader ! {Ref, F} 
           end) || F <- Filenames],
    Sync = fun (_, []) -> ok;
               (S, [_ | _] = Rem) -> receive {Ref, F} -> S(S, Rem -- [F]) end
           end,
    Sync(Sync, Filenames),
    ets:insert(Tab, {schema, MsgKeys}),
    {ok, Tab, Filenames}.


load1(Filename, MsgFrameDef, MsgWithin, Tab) ->
    MsgKeys = lists:keydelete(timestamp, 1, ecomm_scan:framedef_keys(MsgFrameDef)),
    load1(Filename, MsgFrameDef, MsgWithin, MsgKeys, Tab).
load1(Filename, MsgFrameDef, MsgWithin, MsgKeys, Tab) ->
    load1(Filename, MsgFrameDef, MsgWithin, MsgKeys, {undefined, 0, 0}, Tab).
load1(Filename, MsgFrameDef, MsgWithin, MsgKeys, {LastTM, TMIdx, Total}, Tab) ->
    {ok, Bin} = file:read_file(Filename),
    ecomm_scan:fold_chunks(
      fun ([KVs], {LastTM0, TMIdx0, Total0}) ->
              {_, {_, _, _} = TM} = lists:keyfind(timestamp, 1, KVs),
              case MsgWithin(KVs) of
                  true ->
                      {TMKey, TMIdx1} = if LastTM0 == undefined -> {{TM, 1000}, 1001};
                                           TM == LastTM0 -> {{TM, TMIdx0}, TMIdx0 + 1};
                                           TM /= LastTM0 -> {{TM, 0}, 1}
                                        end,
                      Entry = list_to_tuple([TMKey | [proplists:get_value(K, KVs) || K <- MsgKeys]]),
                      ets:insert(Tab, Entry),
                      {cont, {TM, TMIdx1, Total0 + 1}};
                  false ->
                      {cont, {TM, 0, Total0}}
              end
      end, MsgFrameDef, {LastTM, TMIdx, Total}, <<"\n">>, Bin).

    
get_within_keys(Tab, undefined) ->
    case {first_tm(Tab), last_tm(Tab)} of
        {schema, _} -> none;
        {'$end_of_table', _} -> none;
        {T1, T2} -> {ok, {T1, T2}}
    end;
get_within_keys(Tab, {FromTo, ?DATE_TIME_WILDCARD = DT}) when FromTo == from; FromTo == to ->
    get_within_keys(Tab, {FromTo, ecomm_time:tm(DT)});
get_within_keys(Tab, {from, ?TIMESTAMP_WILDCARD = T}) ->
    get_within_keys(Tab, {from, {ecomm_time:tm(T), -1}});
get_within_keys(Tab, {to, ?TIMESTAMP_WILDCARD = T}) ->
    get_within_keys(Tab, {to, {ecomm_time:tm(T), 100000000}});
get_within_keys(Tab, {from, ?TIMESTAMPKEY_WILDCARD = TMKey}) ->
    case get_near_key(Tab, TMKey, next) of
        '$end_of_table' -> none;
        TMKey1 -> {ok, {from, TMKey1}}
    end;
get_within_keys(Tab, {to, ?TIMESTAMPKEY_WILDCARD = TMKey}) ->
    case get_near_key(Tab, TMKey, prev) of
        schema -> none;
        '$end_of_table' -> none;
        TMKey1 -> {ok, {to, TMKey1}}
    end;

get_within_keys(Tab, {to_from, {?DATE_TIME_WILDCARD = DT2, ?DATE_TIME_WILDCARD = DT1}}) ->
    get_within_keys(Tab, {to_from, {ecomm_time:tm(DT2), ecomm_time:tm(DT1)}});
get_within_keys(Tab, {to_from, {?TIMESTAMP_WILDCARD = TM2, ?TIMESTAMP_WILDCARD = TM1}}) ->
    case [get_near_key(Tab, X, Dir) || {X, Dir} <- [{TM1, next}, {TM2, prev}]] of
        [none, none] -> none;
        [T1, none] -> {ok, {T1, T1}};
        [none, T2] -> {ok, {T2, T2}};
        [T1, T2] when T1 =< T2 -> {ok, {to_from, {T2, T1}}};
        [T1, T2] when T1 > T2 -> none
    end;
get_within_keys(Tab, {?DATE_TIME_WILDCARD = DT1, ?DATE_TIME_WILDCARD = DT2}) ->
    get_within_keys(Tab, {ecomm_time:tm(DT1), ecomm_time:tm(DT2)});
get_within_keys(Tab, {?TIMESTAMP_WILDCARD = TM1, ?TIMESTAMP_WILDCARD = TM2}) ->
    get_within_keys(Tab, {{TM1, -1}, {TM2, 100000000}});
get_within_keys(Tab, {?TIMESTAMPKEY_WILDCARD = TMKey1, ?TIMESTAMPKEY_WILDCARD = TMKey2}) ->
    case [get_near_key(Tab, X, Dir) || {X, Dir} <- [{TMKey1, next}, {TMKey2, prev}]] of
        [none, none] -> none;
        [T1, none] -> {ok, {T1, T1}};
        [none, T2] -> {ok, {T2, T2}};
        [T1, T2] when T1 =< T2 -> {ok, {T1, T2}};
        [T1, T2] when T1 > T2 -> none
    end.

get_near_key(Tab, ?TIMESTAMP_WILDCARD = Start, Dir) when Dir == prev orelse Dir == next ->
    get_near_key(Tab, {Start, 0}, Dir);
get_near_key(Tab, {?TIMESTAMP_WILDCARD, _} = Start, Dir) when Dir == prev orelse Dir == next ->
    ets:Dir(Tab, Start).


cont_timebases({error, _}, _RangeKind, _BatchSize) ->
    {<<"">>, <<"">>};
cont_timebases({ok, [], _Cont}, _RangeKind, _BatchSize) ->
    {<<"">>, <<"">>};
cont_timebases({ok, Msgs, Cont}, RangeKind, BatchSize) ->
    IsFullBatch = fun () -> Cont =/= none orelse length(Msgs) == BatchSize end,
    FormatTB = fun (Msg) -> list_to_binary(ecomm_str:fmt("~p", [element(1, Msg)])) end,
    MaybeTB = fun (MsgPos) ->
                      case IsFullBatch() of
                          false -> <<"">>;
                          true -> 
                              FormatTB(case MsgPos of
                                           first -> hd(Msgs);
                                           last -> lists:last(Msgs)
                                       end)
                      end
              end,
    if RangeKind == last orelse RangeKind == from_to ->
            {<<"">>, MaybeTB(last)};
       RangeKind == next ->
            {FormatTB(hd(Msgs)), MaybeTB(last)};
       RangeKind == prev ->
            {MaybeTB(first), FormatTB(lists:last(Msgs))}
    end.


search(empty_none, {_AnyAll, _FrameKVPatterns}, _TMRange, _MsgsBatchSize) ->
    {ok, [], none};
search(Tab, {AnyAll, FrameKVPatterns}, TMRange, MsgsBatchSize) 
  when AnyAll == any orelse AnyAll == all ->
    [{schema, Schema}] = ets:lookup(Tab, schema),
    case get_within_keys(Tab, TMRange) of
        {ok, TMKeys} ->
            MsgWithin = fun (Elt) ->
                                {TM, _} = element(1, Elt),
                                ecomm_time:within(TM, TMKeys) 
                        end,
            try [{ok, _} = re:compile(Pat) || {_, Pat} <- FrameKVPatterns] of
                Pats when is_list(Pats) ->
                    MsgTester = elt_match_tester(Schema, FrameKVPatterns, AnyAll, fun erlang:element/2),
                    EltTester = fun (Elt) -> MsgWithin(Elt) andalso MsgTester(Elt) end,
                    SearchFns = search_fns(TMRange),
                    search1(Tab, SearchFns, EltTester, TMKeys, MsgsBatchSize, [])
            catch
                _:_ ->
                    {error, invalid_regex_filters}
            end;
        none ->
            {ok, [], none}
    end.

search1(Tab, {EtsAdvance, KeyFn, NextTMsFn, TimeCheckFn, ResultFn} = SearchFns, 
        EltTester, TMs, Todo, Acc) ->
    case TimeCheckFn(TMs) of
        false ->
            {ok, ResultFn(Acc), none};
        true ->
            case KeyFn(TMs) of
                K when K == '$end_of_table' orelse K == schema ->
                    {ok, ResultFn(Acc), none};
                {?TIMESTAMP_WILDCARD, _TMIdx} = TM0 ->
                    TM1 = ets:EtsAdvance(Tab, TM0),
                    [Elt] = ets:lookup(Tab, TM0),
                    {Acc1, Todo1} =
                        case EltTester(Elt) of
                            true -> {[Elt | Acc], Todo - 1};
                            false -> {Acc, Todo}
                        end,
                    NextTMs = NextTMsFn(TM1, TMs),
                    if Todo1 == 0 orelse NextTMs == none ->
                            {ok, ResultFn(Acc1), NextTMs};
                       true ->
                            search1(Tab, SearchFns, EltTester, NextTMs, Todo1, Acc1)
                    end
            end
    end.


search_fns({?DATE_TIME_WILDCARD = DT1, ?DATE_TIME_WILDCARD = DT2}) ->
    search_fns({ecomm_time:tm(DT1), ecomm_time:tm(DT2)});
search_fns(undefined) ->
    search_fns({{{0, 0, 0}, -1}, {{100000000, 0, 0}, 0}});
search_fns({?TIMESTAMP_WILDCARD = TM1, ?TIMESTAMP_WILDCARD = TM2}) ->
    search_fns({{TM1, -1}, {TM2, 1000000000}});
search_fns({?TIMESTAMPKEY_WILDCARD, ?TIMESTAMPKEY_WILDCARD}) ->
    {next,                                              % advance
     fun ({TM1, _TM2}) -> TM1 end,                      % get key
     fun ('$end_of_table', _) -> none;                  % next tms
         (NextTM, {_TM1, TM2}) -> {NextTM, TM2} 
     end,
     fun ({TM1, TM2}) -> TM1 =< TM2 end,                 % tm stop
     fun lists:reverse/1};                              % result fun
search_fns({from, ?TIMESTAMPKEY_WILDCARD}) ->
    {next,
     fun ({from, TM}) -> TM end,
     fun ('$end_of_table', _) -> none;
         (NextTM, {from, _TM}) -> {from, NextTM} 
     end,
     fun (_) -> true end,
     fun lists:reverse/1};
search_fns({to, ?TIMESTAMPKEY_WILDCARD}) ->
    {prev, 
     fun ({to, TM}) -> TM end,
     fun (schema, _) -> none;
         (PrevTM, {to, _TM}) -> {to, PrevTM} end,
     fun (_) -> true end,
     fun (Xs) -> Xs end};
search_fns({to_from, {?TIMESTAMPKEY_WILDCARD, ?TIMESTAMPKEY_WILDCARD}}) ->
    {prev, 
     fun ({to_from, {TM2, _TM1}}) -> TM2 end,
     fun (schema, _) -> none;
         (PrevTM, {to_from, {_TM2, TM1}}) -> {to_from, {PrevTM, TM1}} end,
     fun ({to_from, {TM2, TM1}}) -> TM2 >= TM1 end,
     fun (Xs) -> Xs end}.

time_boundaries(Tab) ->
    [{T1, _} = T1Key, {T2, _} = T2Key] = [ecomm_log:Fn(Tab) || Fn <- [first_tm, last_tm]],
    [D1, D2] = [ecomm_time:dt(T, loc) || T <- [T1, T2]],
    {{T1Key, T2Key}, {D1, D2}}.

test_re_checks([], _Elt, all, _GetFn) ->
    true;
test_re_checks([], _Elt, any, _GetFn) ->
    false;
test_re_checks([{Idx, _K, _Pat, CompRe} | Chks], Elt, AllAny, GetFn) ->
    Val = GetFn(Idx, Elt),
    case {re:run(Val, CompRe), AllAny} of
        {{match, _}, any} -> true;
        {nomatch, all} -> false;
        _ -> test_re_checks(Chks, Elt, AllAny, GetFn)
    end.

elt_match_tester(_Schema, [], _AllAny, _GetFn) ->
    fun (_Elt) -> true end;
elt_match_tester(Schema, FrameKVPatterns, AllAny, GetFn)
  when (AllAny == all orelse AllAny == any) andalso is_function(GetFn, 2) ->
    IdxedKeys = lists:zip(lists:seq(2, length(Schema) + 1), Schema),
    RegChecks = [begin
                     {Idx, K} = lists:keyfind(K, 2, IdxedKeys),
                     {ok, CompRe} = re:compile(Pat),
                     {Idx, K, Pat, CompRe}
                 end || {K, Pat} <- FrameKVPatterns],
    fun (Elt) -> test_re_checks(RegChecks, Elt, AllAny, GetFn) end.

%%%%%%%%%%%%%%%%%%%%

first_tm(Tab) -> 
    ets:next(Tab, schema).

last_tm(Tab) ->
    ets:last(Tab).

%%

validate_msg_frame_def([]) ->
    {error, empty_msg_frame_def};
validate_msg_frame_def([_ | _] = Def) ->
    validate_msg_frame_def1(Def).

validate_msg_frame_def1([]) ->
    ok;
validate_msg_frame_def1([K | Rem]) when is_atom(K) ->
    validate_msg_frame_def1(Rem);
validate_msg_frame_def1([{K, Fn} | Rem]) when is_atom(K) andalso is_function(Fn, 2) ->
    validate_msg_frame_def1(Rem);
validate_msg_frame_def1([B | Rem]) when is_binary(B) ->
    case binary:match(B, <<"\n">>) of
        nomatch -> validate_msg_frame_def1(Rem);
        _ -> {error, newline_in_msg_frame_def}
    end.

%%%%%%%%%%

parse_file(Filename, FrameDef) ->
    {ok, Bin} = file:read_file(Filename),
    {_, ResQ} = ecomm_scan:fold_lines(
                  fun (KVs, Q) ->
                          {cont, queue:in(KVs, Q)}
                  end, FrameDef, queue:new(), <<"\n">>, Bin),
    queue:to_list(ResQ).




