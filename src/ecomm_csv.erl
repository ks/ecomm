-module(ecomm_csv).

-export([load_csv_file/2,
         map_csv_file/3, map_csv_file/4,
         map_csv_stream/3, map_csv_stream/4,
         split_rows/1,
         parse_rows/2,
         parse_row/2]).

%%%==============================================
%%% Helpers for CSV
%%%==============================================

-define(SQUOTE, $').
-define(DQUOTE, $").

load_csv_file(Filename, HasHead) ->
    {ok, Bin} = file:read_file(Filename),
    [parse_row(Row) || Row <- parse_rows(Bin, HasHead)].

map_csv_file(FieldFn, RowFn, Filename) ->
    map_csv_file(FieldFn, RowFn, true, Filename).
map_csv_file(FieldFn, RowFn, Head, Filename) ->
    [RowFn([FieldFn(Field) || Field <- RowElts]) || RowElts <- load_csv_file(Filename, Head)].


map_csv_stream(FieldFn, RowFn, Stream) when is_binary(Stream) ->
    map_csv_stream(FieldFn, RowFn, true, Stream).
map_csv_stream(FieldFn, RowFn, Head, Stream) when is_binary(Stream) ->
    Rows = [parse_row(Row) || Row <- parse_rows(Stream, Head)],
    [RowFn([FieldFn(Field) || Field <- RowElts]) || RowElts <- Rows].


split_rows(Rows) when is_binary(Rows) ->
    binary:split(Rows, [<<"\n">>, <<"\r\n">>], [trim, global]).

parse_rows(Rows, HasHead) ->
    parse_rows1(Rows, HasHead).
parse_rows1(Rows, true) ->
    tl(split_rows(Rows));
parse_rows1(Rows, false) ->
    split_rows(Rows).


%% can't just binary:split on $\, as it can contain the comma inside "" of the field
parse_row(Row) when is_binary(Row) ->
    parse_row(Row, []).

parse_row(<<>>, Fields) ->
    lists:reverse(Fields);
parse_row(<<",,", Rest/binary>>, Fields) ->
    parse_row(Rest, [<<>> | Fields]);
parse_row(<<",", Rest/binary>>, Fields) ->
    parse_row(Rest, Fields);
parse_row(<<First, _/binary>> = Row, Fields) when First == ?SQUOTE; First == ?DQUOTE ->
    Sz = size(Row),
    {Index, 1} = binary:match(Row, <<First>>, [{scope, {1, Sz - 1}}]),
    parse_row(binary:part(Row, Index + 1, Sz - Index - 1), [binary:part(Row, 0, Index + 1) | Fields]);
parse_row(Row, Fields) ->
    case binary:match(Row, <<",">>) of
        nomatch ->
            parse_row(<<>>, [Row | Fields]);
        {Index, 1} ->
            Field = binary:part(Row, 0, Index),
            parse_row(binary:part(Row, Index, size(Row) - Index), [Field | Fields])
    end.


