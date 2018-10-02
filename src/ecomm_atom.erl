-module(ecomm_atom).

-export([fmt/2]).

fmt(Format, Args) ->
    binary_to_atom(iolist_to_binary(io_lib:format(Format, Args)), utf8).
