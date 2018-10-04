-module(ecomm_file).

-export([tm/2]).
-export([select/1, select_within/3, file_infos/1]).
-export([updated/1, updates/2]).

-include("ecomm_time.hrl").
-include_lib("kernel/include/file.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tm(Filename, FilenameFrameDef) ->
    {ok, [FilenameKVs]} = ecomm_scan:str(Filename, FilenameFrameDef),
    {timestamp, ?TIMESTAMP_WILDCARD = T} = lists:keyfind(timestamp, 1, FilenameKVs),
    T.

select({Path, [C | _] = Wildcard}) when is_integer(C) ->
    filelib:wildcard(filename:join(Path, Wildcard));
select({Path, [F0 | _] = Fs}) when is_list(F0) ->
    [filename:join(Path, F) || F <- Fs];
select([C | _] = Wildcard) when is_integer(C) ->
    filelib:wildcard(Wildcard);
select([F0 | _] = Filenames0) when is_list(F0) ->
    Filenames0.

select_within(FilenameSel, FilenameFrameDef, ?TIMESTAMP_RANGE_WILDCARD = Ts) ->
    TMFiles = lists:keysort(1, [{tm(F, FilenameFrameDef), F} || F <- select(FilenameSel)]),
    [F || {TMRange, F} <- tm_range_files(TMFiles),ecomm_time:overlaps(TMRange, Ts)];
select_within(FilenameSel, FilenameFrameDef, {?TIMESTAMPKEY_WILDCARD = {T1, _},
                                              ?TIMESTAMPKEY_WILDCARD = {T2, _}}) ->
    select_within(FilenameSel, FilenameFrameDef, {T1, T2});
select_within(FilenameSel, FilenameFrameDef, TimeRangeSpec) when is_tuple(TimeRangeSpec) ->
    select_within(FilenameSel, FilenameFrameDef, ecomm_time:spec_to_tmrange(TimeRangeSpec)).

tm_range_files([{TM1, F1}, {TM2, F2} | TMFs]) ->
    [{{TM1, TM2}, F1} | tm_range_files([{TM2, F2} | TMFs])];
tm_range_files([{TM, F}]) ->
    [{{TM, ecomm_time:tm({loc, calendar:local_time()})}, F}];
tm_range_files([]) ->
    [].

updated({F, #file_info{mtime = ModD}}) ->
    case file:read_file_info(F) of
        {ok, #file_info{mtime = ModD1} = NewFI} ->
            {ModD < ModD1, NewFI};
        {error, enoent} ->
            deleted
    end.

file_infos(Filenames) ->
    lists:foldl(fun (F, {FFIs, NotFound}) ->
                        case file:read_file_info(F) of
                            {ok, FI} -> {[{F, FI} | FFIs], NotFound};
                            {error, enoent} -> {FFIs, [F | NotFound]}
                        end
                end, {[], []}, Filenames).

updates(Filenames, LoadedFFIs) ->
    {UpdFFIs, DelFFIs} =
        lists:foldl(
          fun ({F, _} = FFI, {Upds, Dels}) ->
                  case updated(FFI) of
                      {true, NewFI} -> {[{F, NewFI} | Upds], Dels};
                      {false, _} -> {Upds, Dels};
                      deleted -> {Upds, [FFI | Dels]}
                  end
          end, {[], []}, LoadedFFIs),
    NewFilenames = [F || F <- Filenames, not lists:keymember(F, 1, LoadedFFIs)],
    {NewFFIs, []} = file_infos(NewFilenames),
    {NewFFIs, UpdFFIs, DelFFIs}.
