-module(ecomm_bin).

-export([fmt/2, fmt_oneline/2]).

%%%%%%%%%%

fmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).

fmt_oneline(Format, Args) ->
    list_to_binary(ecomm_str:fmt_oneline(Format, Args)).
