-module(ecomm_scan).

-export([str/2, str/3, bin/2, bin/3, framedef_keys/1]).
-export([line_lf_delim/1]).
-export([incr_bin/4, incr_str/4]).
-export([fold_chunks/5]).

%%%%%%%%%%

%% Example Frame Definition (of trace log):
%% [timestamp, <<"\s">>, txn_id, <<"\n">>, params_in, <<"\n----------\n">>, params_out, <<"\n">>].

framedef_keys(FrameDef) ->
    [K || K <- FrameDef,
          is_atom(K) orelse (is_tuple(K) andalso
                             size(K) == 2 andalso
                             is_atom(element(1, K)))].

pass2_2(_, Arg2) -> Arg2.
pass2_2_to_str(_, Arg2) -> binary_to_list(Arg2).

line_lf_delim(Bin) ->
    line_lf_delim1(Bin, 0, size(Bin), size(Bin)).
line_lf_delim1(Bin, StartPos, RemSize, Size) ->
    case binary:match(Bin, <<"\n">>, [{scope, {StartPos, RemSize}}]) of
        nomatch ->
            incomplete;
        {Pos, End} when Pos + End + 1 == Size ->
            {Bin, <<>>};
        {Pos, Len} ->
            NextChar = binary:at(Bin, Pos + Len + 1),
            Continue = lists:member(NextChar, [$\s, $\t, $\r]),
            Offset = Pos + Len,
            if Continue -> line_lf_delim1(Bin, Offset, Size - Offset, Size);
               true -> {binary:part(Bin, 0, Pos), binary:part(Bin, Offset, Size - Offset)}
            end
    end.


str(X, FrameDef) when is_list(FrameDef) ->
    str(X, FrameDef, undefined).
str(Str, FrameDef, MaxFrames) when is_list(Str), is_list(FrameDef) ->
    str(list_to_binary(Str), FrameDef, MaxFrames);
str(Bin, FrameDef, MaxFrames) when is_binary(Bin), is_list(FrameDef) ->
    parse1(Bin, FrameDef, {[], queue:new(), MaxFrames, FrameDef}, fun pass2_2_to_str/2).

bin(X, FrameDef) when is_list(FrameDef) ->
    bin(X, FrameDef, undefined).
bin(Str, FrameDef, MaxFrames) when is_list(Str), is_list(FrameDef) ->
    bin(list_to_binary(Str), FrameDef, MaxFrames);
bin(Bin, FrameDef, MaxFrames) when is_binary(Bin), is_list(FrameDef) ->
    parse1(Bin, FrameDef, {[], queue:new(), MaxFrames, FrameDef}, fun pass2_2/2).

parse1(<<>>, [], {FrameKVs, ResFrames, _, _}, _) ->
    {ok, queue:to_list(push_frame(FrameKVs, ResFrames))};
parse1(Bin, _, {_, ResFrames, 0, _FrameDef}, _) ->
    {ok, queue:to_list(ResFrames), Bin};
parse1(Bin, [], {FrameKVs, ResFrames, undefined, FrameDef}, ConvFn) ->
    parse1(Bin, FrameDef, {[], push_frame(FrameKVs, ResFrames), undefined, FrameDef}, ConvFn);
parse1(Bin, [], {FrameKVs, ResFrames, N, FrameDef}, ConvFn) when is_integer(N), N > 0 ->
    parse1(Bin, FrameDef, {[], push_frame(FrameKVs, ResFrames), N - 1, FrameDef}, ConvFn);
parse1(Bin, [K | FDef], {FrameKVs, ResFrames, MaxFrames, FrameDef}, ConvFn)
  when is_atom(K) ->
    parse1(Bin, [{K, ConvFn} | FDef], {FrameKVs, ResFrames, MaxFrames, FrameDef}, ConvFn);
parse1(Bin, [{K, Fn}], {FrameKVs, ResFrames, MaxFrames, FrameDef}, ConvFn)
  when is_atom(K), is_function(Fn, 2) ->
    parse1(<<>>, [], {[{K, Fn(K, Bin)} | FrameKVs], ResFrames, MaxFrames, FrameDef}, ConvFn);
parse1(<<>>, [_ | _] = FDef, {FrameKVs, ResFrames, _, _}, _) ->
    {incomplete, {FDef, FrameKVs, queue:to_list(ResFrames)}};
parse1(Bin, [{K, Fn}, DelimFn | FDef], {FrameKVs, ResFrames, MaxFrames, FrameDef}, ConvFn)
  when is_function(DelimFn), is_atom(K), is_function(Fn, 2) ->
    case DelimFn(Bin) of
        incomplete ->
            {incomplete, {FDef, FrameKVs, queue:to_list(ResFrames)}};
        {EltBin, RemBin} ->
            FrameKVs1 = [{K, Fn(K, EltBin)} | FrameKVs],
            parse1(RemBin, FDef, {FrameKVs1, ResFrames, MaxFrames, FrameDef}, ConvFn)
    end;
parse1(Bin, [{K, Fn}, <<Delim/binary>> | FDef], {FrameKVs, ResFrames, MaxFrames, FrameDef}, ConvFn)
  when is_atom(K), is_function(Fn, 2) ->
    case binary:split(Bin, Delim) of
        [_] ->
            {incomplete, {[K, <<Delim/binary>> | FDef], FrameKVs, queue:to_list(ResFrames)}};
        [V, RemBin] ->
            FrameKVs1 = [{K, Fn(K, V)} | FrameKVs],
            parse1(RemBin, FDef, {FrameKVs1, ResFrames, MaxFrames, FrameDef}, ConvFn)
    end;
parse1(Bin, [EatFn | FDef], {FrameKVs, ResFrames, MaxFrames, FrameDef}, ConvFn)
  when is_function(EatFn, 1) ->
    {<<>>, RemBin} = EatFn(Bin),
    parse1(RemBin, FDef, {FrameKVs, ResFrames, MaxFrames, FrameDef}, ConvFn).

push_frame(FrameKVs, Frames) ->
    queue:in(lists:reverse(FrameKVs), Frames).

%%%%%%%%%%

%% This can't handle log lines spanning multiple lines

incr_bin(FrameFn, In, FrameDef, State) ->
    incr1(fun bin/3, FrameFn, In, FrameDef, State).
incr_str(FrameFn, In, FrameDef, State) ->
    incr1(fun str/3, FrameFn, In, FrameDef, State).

incr1(ToType, FrameFn, In, FrameDef, State) when is_function(ToType, 3) ->
    case ToType(In, FrameDef, 1) of
        {ok, [KVs]} ->
            FrameFn(KVs, State);
        {ok, [KVs], RemBin} ->
            case FrameFn(KVs, State) of
                {cont, NextState} -> incr1(ToType, FrameFn, RemBin, FrameDef, NextState);
                stop -> {stop, RemBin, FrameDef, State}
            end;
        {incomplete, {RemFrameDef, FrameKVs, Frames}} ->
            {incomplete, {RemFrameDef, FrameKVs, Frames}, State}
    end.

%%%%%%%%%%

%% Delim should generally be just endline char(s) (0xA, or 0xD,0xA)
fold_chunks(KVsFn, LineFrameDef, State, Delim, Bin) ->
    fold_chunks1(KVsFn, LineFrameDef, State, {<<>>, undefined}, Delim, Bin).

fold_chunks1(KVsFn, LineFrameDef, State, {CurrLineBuf, PrevLine}, Delim, Bin) ->
    case binary:split(Bin, Delim) of
        [_] ->
            CurrLineBuf1 = <<CurrLineBuf/binary, Bin/binary>>,
            case {bin(CurrLineBuf1, LineFrameDef), PrevLine} of
                {{incomplete, _}, undefined} ->
                    {incomplete, State};
                {{incomplete, _}, {PrevLineBuf, _}} ->
                    CurrLineBuf2 = <<PrevLineBuf/binary, CurrLineBuf1/binary>>,
                    fold_chunks1(KVsFn, LineFrameDef, State, {CurrLineBuf2, undefined}, Delim, <<>>);
                {{ok, CurrKVs}, undefined} ->
                    KVsFn(CurrKVs, State);
                {{ok, CurrKVs}, {_PrevLineBuf, PrevKVs}} ->
                    case KVsFn(PrevKVs, State) of
                        {cont, State1} -> KVsFn(CurrKVs, State1);
                        {stop, State1} -> {stop, State1}
                    end
            end;
        [CurrChunk, RemBin] ->
            CurrLineBuf1 = <<CurrLineBuf/binary, CurrChunk/binary>>,
            case {bin(CurrLineBuf1, LineFrameDef), PrevLine} of
                {{incomplete, _}, undefined} ->
                    fold_chunks1(KVsFn, LineFrameDef, State, {CurrLineBuf1, PrevLine}, Delim, RemBin);
                {{incomplete, _}, {PrevLineBuf, _}} ->
                    CurrLineBuf2 = <<PrevLineBuf/binary, CurrLineBuf1/binary>>,
                    fold_chunks1(KVsFn, LineFrameDef, State, {CurrLineBuf2, undefined}, Delim, RemBin);
                {{ok, CurrKVs}, undefined} ->
                    BuffersState = {<<>>, {CurrLineBuf1, CurrKVs}},
                    fold_chunks1(KVsFn, LineFrameDef, State, BuffersState, Delim, RemBin);
                {{ok, CurrKVs}, {_PrevLineBuf, PrevKVs}} ->
                    case KVsFn(PrevKVs, State) of
                        {cont, State1} ->
                            BuffersState = {<<>>, {CurrLineBuf1, CurrKVs}},
                            fold_chunks1(KVsFn, LineFrameDef, State1, BuffersState, Delim, RemBin);
                        {stop, State1} ->
                            {stop, State1}
                    end
            end
    end.
