-module(ecomm_str).

-export([eval/2]).
-export([parse/1]).
-export([fmt/2, fmt_oneline/2]).
-export([get_input/4]).

%%%%%%%%%%

eval(String, Env) when is_list(String), is_list(Env) ->
    String1 = case lists:last(String) of
                  $. -> String;
                  _ -> String ++ "."
              end,
    {ok, Scanned, _} = erl_scan:string(String1),
    {ok, Parsed} = erl_parse:parse_exprs(Scanned),
    erl_eval:exprs(Parsed, []).

fmt(Format, Args) ->
    binary_to_list(iolist_to_binary(io_lib:format(Format, Args))).

parse({nonneg_int, Str}) when is_list(Str) ->
    try list_to_integer(Str) of
        I when I >= 0 -> {ok, I};
        I when I < 0 -> {error, negative}
    catch
        _:_ ->
            {error, not_int}
    end.
                 
   
fmt_oneline(Format, Args) ->
    fmt_oneline1(fmt(Format, Args), [], false).

fmt_oneline1([], Acc, _) ->
    lists:reverse(Acc);
fmt_oneline1([$\n], Acc, _) ->
    fmt_oneline1([], [$\n | Acc], false);
fmt_oneline1([$\n | Cs], Acc, _) ->
    fmt_oneline1(Cs, Acc, true);
fmt_oneline1([$\s | Cs], Acc, true) ->
    fmt_oneline1(Cs, Acc, true);
fmt_oneline1([C | Cs], Acc, _) ->
    fmt_oneline1(Cs, [C | Acc], false).


%%%%%%%%%%

get_input(Prompt, ErrFmt, Strip, ParseFn) when Strip == strip orelse Strip == no_strip ->
    String = string:strip(io:get_line(standard_io, Prompt), right, $\n),
    Stripped = if Strip == strip -> string:strip(String, both);
                  Strip == no_strip -> String
               end,
    case ParseFn(Stripped) of
        {ok, Val} -> 
            Val;
        {error, _} -> 
            io:format(ErrFmt, [Stripped]),
            get_input(Prompt, ErrFmt, Strip, ParseFn)
    end.
