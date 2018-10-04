-module(ecomm_time).

-include("ecomm_time.hrl").

-export([tm/1, tm/2, dt/2, dt_ext/2]).
-export([gr_secs/1, gr_secs_sub/2, uni_loc_diff_secs/0, seconds/1]).
-export([within/2, overlaps/2, spec_to_tmrange/1, spec_to_tmrange/2]).
-export([range/3, range/4]).
-export([parse/2, parse/3, valid/2]).

-export([t/0]).

%%%%%%%%%%

gr_secs(?DATE_TIME_WILDCARD = DT) ->
    %% 62167219200 == calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})
    calendar:datetime_to_gregorian_seconds(DT) - 62167219200.

gr_secs_sub(DT1, DT2) ->
    calendar:datetime_to_gregorian_seconds(DT1) - calendar:datetime_to_gregorian_seconds(DT2).

uni_loc_diff_secs() ->
    apply(ecomm_time, gr_secs_sub, [calendar:Time() || Time <- [universal_time, local_time]]).

%%%%

dt(?TIMESTAMP_WILDCARD = TM, uni) ->
    calendar:now_to_universal_time(TM);
dt(?TIMESTAMP_WILDCARD = TM, loc) ->
    calendar:now_to_local_time(TM);
dt({?TIMESTAMP_WILDCARD = TM1, ?TIMESTAMP_WILDCARD = TM2}, OutTMSpec) ->
    {dt(TM1, OutTMSpec), dt(TM2, OutTMSpec)}.

dt_ext({_, _, MS} = ?TIMESTAMP_WILDCARD = TM, uni) ->
    {D, {T1, T2, T3}} = calendar:now_to_universal_time(TM),
    {D, {T1, T2, T3, MS}};
dt_ext({_, _, MS} = ?TIMESTAMP_WILDCARD = TM, loc) ->
    {D, {T1, T2, T3}} = calendar:now_to_local_time(TM),
    {D, {T1, T2, T3, MS}};
dt_ext({?TIMESTAMP_WILDCARD = TM1, ?TIMESTAMP_WILDCARD = TM2}, OutTMSpec) ->
    {dt_ext(TM1, OutTMSpec), dt_ext(TM2, OutTMSpec)}.

%% converts to UNI timestamp or TMKey (timestamp + index to for uniqueness)
tm({Mark, D}) when Mark == uni orelse Mark == loc ->
    tm({Mark, D}, uni_loc_diff_secs()).
tm({uni, ?DATE_TIME_WILDCARD = D}, _) ->
    Secs = gr_secs(D),
    {Secs div 1000000, Secs rem 1000000, 0};
tm({loc, ?DATE_TIME_WILDCARD = D}, UniLocDiffSecs) -> % converts loc DT to uni timestamp
    Secs = gr_secs(D) + UniLocDiffSecs,
    {Secs div 1000000, Secs rem 1000000, 0};
tm({Mark, ?DATE_TIME_EXT_WILDCARD = {{Y, Mo, D}, {H, Mi, S, USecs}}}, UniLocDiffSecs)
  when Mark == uni orelse Mark == loc ->
    {T1, T2, 0} = tm({Mark, {{Y, Mo, D}, {H, Mi, S}}}, UniLocDiffSecs),
    {T1, T2, USecs};
tm({Mark, {D1, D2}}, UniLocDiffSecs) when is_tuple(D1), is_tuple(D2) ->
    {tm({Mark, D1}, UniLocDiffSecs), tm({Mark, D2}, UniLocDiffSecs)};
tm({Mark, {Key, ?DATE_TIME_EXT_WILDCARD = D}}, UniLocDiffSecs) when is_atom(Key) ->
    {Key, tm({Mark, D}, UniLocDiffSecs)};
tm({Mark, {Key, ?DATE_TIME_EXT_RANGE_WILDCARD = Ds}}, UniLocDiffSecs) when is_atom(Key) ->
    {Key, tm({Mark, Ds}, UniLocDiffSecs)};
tm({Mark, {Key, {?DATE_TIME_EXT_RANGE_WILDCARD = Ds, TMIdx}}}, UniLocDiffSecs) when is_atom(Key) ->
    {Key, {tm({Mark, Ds}, UniLocDiffSecs), TMIdx}};
tm({Mark, {Key, ?DATE_TIME_WILDCARD = D}}, UniLocDiffSecs) when is_atom(Key) ->
    {Key, tm({Mark, D}, UniLocDiffSecs)};
tm({Mark, {Key, ?DATE_TIME_RANGE_WILDCARD = Ds}}, UniLocDiffSecs) when is_atom(Key) ->
    {Key, tm({Mark, Ds}, UniLocDiffSecs)};
tm({{TA1, TA2, TA3}, {TB1, TB2, TB3}} = ?TIMESTAMP_RANGE_WILDCARD = Ts, _UniLocDiffSecs) ->
    true = lists:all(fun (X) -> is_integer(X) andalso X >= 0 end, [TA1, TA2, TA3, TB1, TB2, TB3]),
    Ts.

-define(tmkey_max_idx, 100000000).

spec_to_tmrange(?TIMESTAMPKEY_RANGE_WILDCARD = TKs) ->
    TKs;
spec_to_tmrange(?TIMESTAMP_RANGE_WILDCARD = Ts) ->
    Ts;
spec_to_tmrange({from, ?TIMESTAMP_WILDCARD = T1}) ->
    {T1, os:timestamp()};
spec_to_tmrange({from, {?TIMESTAMP_WILDCARD = T1, TIdx}}) ->
    {{T1, TIdx}, {os:timestamp(), ?tmkey_max_idx}};
spec_to_tmrange({to, ?TIMESTAMP_WILDCARD = T2}) ->
    {{0, 0, 0}, T2};
spec_to_tmrange({to, ?TIMESTAMPKEY_WILDCARD = TK2}) ->
    {{{0, 0, 0}, -1}, TK2};
spec_to_tmrange({to_from, ?TIMESTAMP_RANGE_WILDCARD = {T2, T1}}) ->
    {T1, T2};
spec_to_tmrange({to_from, ?TIMESTAMPKEY_RANGE_WILDCARD = {T2, T1}}) ->
    {T1, T2};
spec_to_tmrange({?TIMESTAMP_WILDCARD = T1, ?TIMESTAMPKEY_WILDCARD = TK2}) ->
    {{T1, -1}, TK2};
spec_to_tmrange({?TIMESTAMPKEY_WILDCARD = TK1, ?TIMESTAMP_WILDCARD = T2}) ->
    {TK1, {T2, ?tmkey_max_idx}};
spec_to_tmrange({Mark, Spec}) when Mark == uni orelse Mark == loc ->
    spec_to_tmrange({Mark, Spec}, uni_loc_diff_secs()).

spec_to_tmrange({Mark, Spec}, ULDS) when Mark == uni orelse Mark == loc ->
    spec_to_tmrange(tm({Mark, Spec}, ULDS));
spec_to_tmrange(Spec, _ULDS) ->
    spec_to_tmrange(Spec).



within(?TIMESTAMP_WILDCARD = T, {?TIMESTAMPKEY_WILDCARD = {T1, _},
                                 ?TIMESTAMPKEY_WILDCARD = {T2, _}}) ->
    within(T, {T1, T2});
within(?TIMESTAMP_WILDCARD = T, {?TIMESTAMP_WILDCARD = T1, ?TIMESTAMPKEY_WILDCARD = {T2, _}}) ->
    within(T, {T1, T2});
within(?TIMESTAMP_WILDCARD = T, {?TIMESTAMPKEY_WILDCARD = {T1, _}, ?TIMESTAMP_WILDCARD = T2}) ->
    within(T, {T1, T2});
within(?TIMESTAMP_WILDCARD = T, ?TIMESTAMP_RANGE_WILDCARD = {T1, T2}) ->
    T1 =< T andalso T =< T2;

within(?TIMESTAMP_WILDCARD = T, {from, {?TIMESTAMP_WILDCARD = T1, _}}) ->
    within(T, {from, T1});
within(?TIMESTAMP_WILDCARD = T, {from, ?TIMESTAMP_WILDCARD = T1}) ->
    T1 =< T;
within(?TIMESTAMP_WILDCARD = T, {to, {?TIMESTAMP_WILDCARD = T2, _}}) ->
    within(T, {to, T2});
within(?TIMESTAMP_WILDCARD = T, {to, ?TIMESTAMP_WILDCARD = T2}) ->
    T =< T2;
within(?TIMESTAMP_WILDCARD = T, {to_from, ?TIMESTAMPKEY_RANGE_WILDCARD = {{T2, _}, {T1, _}}}) ->
    within(T, {to_from, {T1, T2}});
within(?TIMESTAMP_WILDCARD = T, {to_from, ?TIMESTAMP_RANGE_WILDCARD = {T2, T1}}) ->
    within(T, {T1, T2}).

overlaps(?TIMESTAMP_RANGE_WILDCARD = {From, To}, ?TIMESTAMP_RANGE_WILDCARD = {TM1, TM2}) ->
    within(From, {TM1, TM2}) orelse within(To, {TM1, TM2}) orelse (From =< TM1 andalso To >= TM2).

%%%%%%%%%%

seconds({X, Units}) when is_integer(X), X >= 0, is_atom(Units) ->
    case Units of
        seconds -> X;
        minutes -> X * 60;
        hours -> X * 60 * seconds({1, minutes});
        days -> X * 24 * seconds({1, hours});
        weeks -> X * 7 * seconds({1, days});
        months -> X * 30 * seconds({1, days});
        years -> X * 365 * seconds({1, days})
    end.

range(dt, {Tok, X, Units}, uni) when X >= 0 andalso (Tok == last orelse Tok == first) ->
    {uni, range(dt, Tok, calendar:universal_time(), seconds({X, Units}))};
range(dt, {Tok, X, Units}, loc) when X >= 0 andalso (Tok == last orelse Tok == first) ->
    {loc, range(dt, Tok, calendar:local_time(), seconds({X, Units}))}.

range(dt, last, ?DATE_TIME_WILDCARD = BaseDT, Seconds) ->
    T1Secs = calendar:datetime_to_gregorian_seconds(BaseDT),
    {calendar:gregorian_seconds_to_datetime(T1Secs - Seconds), BaseDT};
range(dt, first, ?DATE_TIME_WILDCARD = BaseDT, Seconds) ->
    T1Secs = calendar:datetime_to_gregorian_seconds(BaseDT),
    {BaseDT, calendar:gregorian_seconds_to_datetime(T1Secs + Seconds)}.

%%%%%%%%%%

valid(dt, {{Y, Mo, D}, {H, Mi, S}}) ->
    calendar:valid_date({Y, Mo, D}) andalso
        lists:all(fun ({X, Lim}) -> is_integer(X) andalso X >= 0 andalso X < Lim end,
                  [{H, 24}, {Mi, 60}, {S, 60}]).

parse(dt, Bin) when is_binary(Bin) ->
    parse(dt, binary_to_list(Bin));
parse(dt, Str) when is_list(Str) ->
    Toks = string:tokens(Str, ":/-_. "),
    ToksLen = length(Toks),
    if ToksLen =< 0 ->
            {error, too_short};
       ToksLen > 7 ->
            {error, too_long};
       true ->
            try [begin I = list_to_integer(X), true = I >= 0, I end || X <- Toks] of
                Is ->
                    [Y, Mo, D, H, Mi, S, Ms] =
                        case length(Is) of
                            7 -> Is;
                            N when N >= 3 -> Is ++ lists:duplicate(7 - N, 0);
                            N when N < 3 -> Is ++ lists:duplicate(3 - N, 1) ++ lists:duplicate(4, 0)
                        end,
                    DT = {{Y, Mo, D}, {H, Mi, S}},
                    case valid(dt, DT) of
                        true -> {ok, if Ms == 0 -> DT;
                                        true -> {{Y, Mo, D}, {H, Mi, S, Ms * 1000}}
                                     end};
                        false -> {error, invalid_datetime}
                    end
            catch
                _:_ ->
                    {error, invalid_datetime}
            end
    end.

parse(dt, Bin, Def) when is_binary(Bin) andalso (Def == uni orelse Def == loc) ->
    parse(dt, binary_to_list(Bin), Def);
parse(dt, [$u, $n, $i, $\s | Str], Def) when (Def == uni orelse Def == loc) ->
    parse1(dt, Str, uni);
parse(dt, [$l, $o, $c, $\s | Str], Def) when (Def == uni orelse Def == loc) ->
    parse1(dt, Str, loc);
parse(dt, Str, Def) when (Def == uni orelse Def == loc) ->
    parse1(dt, Str, Def).

parse1(dt, Str, ResTMSpec) ->
    case parse(dt, Str) of
        {ok, D} -> {ok, {ResTMSpec, D}};
        Err -> Err
    end.

%%%%%%%%%%

%% works in gmt+2 zone

t() ->
    [io:format("test ~p -> ~p~n", [X, catch t(X)]) || X <- [1, 2, 3, 4, 5, 6]].

t(1) ->
    UniDT = {{2015, 8, 8}, {12,0,0}},
    UniTM = ecomm_time:tm({uni, UniDT}),
    true = UniDT == calendar:now_to_universal_time(UniTM);
t(2) ->
    LocDT = {{2015, 8, 8}, {12, 0, 0}},
    UniTM = ecomm_time:tm({loc, LocDT}),
    true = {{2015, 8, 8}, {10, 0, 0}} == calendar:now_to_universal_time(UniTM);
t(3) ->
    UniDT = {{2015, 8, 8}, {12,0,0}},
    UniTM = ecomm_time:tm({uni, UniDT}),
    true = {{2015, 8, 8}, {14,0,0}} == calendar:now_to_local_time(UniTM);
t(4) ->
    LocDT = {{2015, 8, 8}, {12,0,0}},
    UniTM = ecomm_time:tm({loc, LocDT}),
    true = LocDT == calendar:now_to_local_time(UniTM);
t(5) ->
    UniDT = {{2015, 8, 8}, {12,0,0}},
    true = UniDT == dt(tm({uni, UniDT}), uni);
t(6) ->
    UniDT = {{2015, 8, 8}, {12,0,0}},
    true = {{2015, 8, 8}, {14,0,0}} == dt(tm({uni, UniDT}), loc).
