-module(ecomm_proto_sip).

-export([sip_parser/0]).
-export([decode/1, decode/2]).
-export([decode_name_address/1, decode_address/1]).
-export([decode_parameters/1]).

-export([encode/1]).
-export([encode_address/1]).

%%%%%%%%%%

-define(DEC_NL, <<"\r\n">>).
-define(DEC_BL, <<"\r\n\r\n">>).
-define(ENC_NL, <<"\r\n">>).
-define(ENC_BL, <<"\r\n\r\n">>).
-define(ENC_MIME_BOUNDARY, <<"MIME_boundary_h7Fc5n9R02KqpLiJ6FN8">>).

-record(sip_body, 
        {ctype          :: binary(),
         headers = []   :: [{binary(), binary()}], 
         content = <<>> :: binary()}).

-record(sip_parser, 
        {state          :: meta | headers | body,
         clen           :: undefined | integer(),
         method         :: undefined | binary(),
         uri            :: undefined | binary(),
         vsn            :: undefined | float(),
         headers = []   :: [{binary(), binary()}],
         bodies  = []   :: [#sip_body{}],
         data_in = <<>> :: binary(),
         request = <<>> :: binary()}).
                     

sip_parser() ->
    #sip_parser{state = meta}.


decode(Packet) ->
    decode(Packet, sip_parser()).

decode(Packet, #sip_parser{state = meta, data_in = DataIn, request = <<>>} = P) ->
    case binary:split(<<DataIn/binary, Packet/binary>>, ?DEC_NL) of
        [DataIn1] ->
            {incomplete, P#sip_parser{data_in = DataIn1}};
        [MetaChunk, DataIn2] ->
            {Method, Uri, Vsn} = decode_meta(MetaChunk),
            decode(<<>>, P#sip_parser{state = headers, 
                                      method = Method,
                                      uri = Uri,
                                      vsn = Vsn,
                                      data_in = DataIn2,
                                      request = <<MetaChunk/binary, ?DEC_NL/binary>>})
    end;
decode(Packet, #sip_parser{state = headers, data_in = DataIn, request = Request} = P) ->
    case binary:split(<<DataIn/binary, Packet/binary>>, ?DEC_BL) of
        [DataIn1] ->
            {incomplete, P#sip_parser{data_in = DataIn1}};
        [HeadersChunk, DataIn2] ->
            Headers = decode_headers(HeadersChunk),
            {_, CL} = lists:keyfind(<<"Content-Length">>, 1, Headers),
            Request1 = <<Request/binary, HeadersChunk/binary, ?DEC_BL/binary>>,
            decode(<<>>, P#sip_parser{state = body,
                                      clen = binary_to_integer(CL),
                                      headers = Headers,
                                      data_in = DataIn2, 
                                      request = Request1})
    end;    
decode(Packet, #sip_parser{state = body, 
                           clen = CLen, 
                           headers = Headers, 
                           data_in = DataIn, 
                           request = Request} = P) ->
    DataIn1 = <<DataIn/binary, Packet/binary>>,
    if size(DataIn1) < CLen ->
            {incomplete, P#sip_parser{data_in = DataIn1}};
       true ->
            BodyChunk = binary:part(DataIn1, {0, CLen}),
            NextDataIn = case size(DataIn1) == CLen of
                             true -> <<>>;
                             false -> binary:part(DataIn1, {CLen, size(DataIn1) - CLen})
                         end,
            Request1 = <<Request/binary, BodyChunk/binary>>,
            P1 = P#sip_parser{data_in = <<>>, request = Request1},
            P2 = case lists:keyfind(<<"Content-Type">>, 1, Headers) of
                     false ->
                         P1;
                     {_, <<"multipart/mixed;boundary=", Tag/binary>>} -> 
                         P1#sip_parser{bodies = decode_bodies(BodyChunk, Tag)};
                     {_, CType} = Header ->
                         P1#sip_parser{bodies = [#sip_body{ctype = CType,
                                                           headers = [Header],
                                                           content = BodyChunk}]}
                 end,
            {construct_request_kvs(P2), #sip_parser{state = meta, data_in = NextDataIn}}
    end.

%%

decode_meta(Chunk) ->
    [Method, Uri, <<"SIP/", VsnBin/binary>>] = binary:split(Chunk, <<" ">>, [global]),
    {Method, Uri, binary_to_float(VsnBin)}.

decode_headers(Chunk) ->
    [{_, _} = parse_keyval(Line, <<": ">>) || Line <- binary:split(Chunk, ?DEC_NL, [global])].

decode_bodies(Chunk, Tag) ->
    [Chunks0 | _] = binary:split(Chunk, <<"--", Tag/binary, "--", ?DEC_NL/binary>>, [global, trim]),
    Chunks = binary:split(Chunks0, <<"--", Tag/binary, ?DEC_NL/binary>>, [global, trim]),
    [case binary:split(BodyChunk, ?DEC_BL) of
         [_] ->
             #sip_body{ctype = undefined, headers = [], content = BodyChunk};
         [PartHeadersChunk, PartContent] ->
             PartHeaders = decode_headers(PartHeadersChunk),
             #sip_body{ctype = ecomm_pl:getv(<<"Content-Type">>, PartHeaders),
                       headers = PartHeaders,
                       content = PartContent}
     end || BodyChunk <- Chunks].

construct_request_kvs(#sip_parser{} = P) ->
    [{meta, [{type, request}, 
             {method, P#sip_parser.method}, 
             {uri, P#sip_parser.uri},
             {vsn, P#sip_parser.vsn}]},
     {headers,
      [case Name of
           <<"To">> -> {Name, decode_name_address(Val)};
           <<"From">> -> {Name, decode_name_address(Val)};
           _ -> {Name, Val}
       end || {Name, Val} <- P#sip_parser.headers]} |
     case P#sip_parser.bodies of
         [] -> 
             [];
         [_ | _] = Bodies->
             [{body, [{type, multipart}, 
                      {parts, [[{headers, PartHs}, {content, PartContent}] ||
                                  #sip_body{headers = PartHs , content = PartContent} <- Bodies,
                                  PartContent /= <<>>]}]}]
     end] ++ [{request, P#sip_parser.request}].

%%%%%%%%%%

decode_name_address(Bin) when is_binary(Bin) ->
    case decode_name_address(Bin, [<<"<sip:">>, <<"<urn:">>, <<"<tel:">>]) of
        Res when is_list(Res) -> Res;
        {error, invalid_sip_uri} -> decode_address(binary:list_to_bin(["<", Bin, ">"]))
    end.

decode_name_address(Bin, []) when is_binary(Bin) ->
    {error, invalid_name_address};
decode_name_address(Bin, Delimiters) when is_binary(Bin) ->
    case binary:split(Bin, Delimiters) of
        [<<>>, _] ->
            decode_address(Bin);
        [Name, _] ->
            NameSz = size(Name),
            [{display_name, strip_junk_chars(binary_to_list(Name))} |
             decode_address(binary:part(Bin, NameSz, size(Bin) - NameSz))];
        _ ->
            {error, invalid_sip_uri}
    end.

decode_address(<<"<urn:", ServiceChunk/binary>>) ->
    decode_address1(urn, ServiceChunk);
decode_address(<<"<tel:", NumberChunk/binary>>) ->
    decode_address1(tel, NumberChunk);
decode_address(<<"<sip:", AddressChunk/binary>>) ->
    decode_address1(sip, AddressChunk);
decode_address(<<S:3/binary, _/binary>> = Addr) when S == <<"urn">>; S == <<"tel">>; S == <<"sip">> ->
    decode_address(<<"<", Addr/binary, ">">>);
decode_address(<<UnknownTarget/binary>>) ->
    [{scheme, unknown}, {target, UnknownTarget}].


decode_address1(Scheme, AddressChunk) when is_atom(Scheme), is_binary(AddressChunk) ->
    {Target, HostPortKVs, ParamsKVs} = split_address(AddressChunk),
    ParamsKVs1 = if ParamsKVs == [] -> []; true -> [{parameters, ParamsKVs}] end,
    lists:append([[{scheme, Scheme}, {target, binary_to_list(Target)}], HostPortKVs, ParamsKVs1]).
    

address_host_port_kvs(HostPortChunk) when is_binary(HostPortChunk) ->
    case HostPortChunk of
        <<>> ->
            [];
        _ -> 
            [Host | PortChunk] = binary:split(HostPortChunk, <<":">>),
            [{host, binary_to_list(Host)} | 
             case PortChunk of
                 [] -> [];
                 [PortNo] -> [{port, binary_to_integer(PortNo)}]
             end]
    end.

split_address(TargetChunk) when is_binary(TargetChunk) ->
    [TargetParamsInChunk, ParamsOutChunk] = binary:split(TargetChunk, <<">">>),
    ParamsOut = decode_parameters(ParamsOutChunk),
    case {binary:split(TargetParamsInChunk, <<";">>), binary:split(TargetParamsInChunk, <<"@">>)} of
        {[Target1, ParamsInChunk], [Target2 | _]} when size(Target1) < size(Target2) ->
            {Target1, [], ParamsOut ++ decode_parameters(ParamsInChunk)};
        {[Target1, _], [Target2, HostPort0]} when size(Target2) < size(Target1) ->
            [HostPort, ParamsInChunk] = binary:split(HostPort0, <<";">>),
            {Target2, address_host_port_kvs(HostPort), ParamsOut ++ decode_parameters(ParamsInChunk)};
        {[Target1], [Target2, HostPort]} when size(Target2) < size(Target1) ->
            {Target2, address_host_port_kvs(HostPort), ParamsOut};
        {[Target0], [Target0]} ->
            {Target0, [], ParamsOut}
    end.


decode_parameters(<<>>) ->
    [];
decode_parameters(<<";", ParamsChunk/binary>>) ->
    decode_parameters(ParamsChunk);
decode_parameters(ParamsChunk) ->
    [list_to_tuple([binary_to_list(B) || B <- binary:split(Chunk, <<"=">>)]) ||
        Chunk <- binary:split(ParamsChunk, <<";">>, [global])].

strip_junk_chars(Name) when is_list(Name) ->
    Name1 = strip_junk_chars1(Name),
    lists:reverse(strip_junk_chars1(lists:reverse(Name1))).
strip_junk_chars1("\"" ++ Name) ->
    strip_junk_chars1(Name);
strip_junk_chars1(" " ++ Name) ->
    strip_junk_chars1(Name);
strip_junk_chars1(Name) ->
    Name.

parse_keyval(Line, KVSeparator) ->
    list_to_tuple(binary:split(Line, KVSeparator)).

%%%==============================================
%%% Basic encoder for SIP replies
%%%==============================================

encode(Props) when is_list(Props) ->
    {Reply, []} = ecomm_pl:mgetv([status, code, headers, body], Props),
    encode(list_to_tuple([sip_reply | Reply]));

encode({sip_reply, Status, Code, Headers}) 
  when is_binary(Status), is_integer(Code), is_list(Headers) ->
    encode({sip_reply, Status, Code, Headers, <<>>});

%% supplies Content-Length header if not present
encode({sip_reply, Status, Code, Headers, Body})
  when is_binary(Status), is_integer(Code), is_list(Headers), is_binary(Body) ->
    Headers1 = case ecomm_pl:getv(<<"Content-Length">>, Headers) of
                   undefined ->
                       CLenBin = integer_to_binary(size(Body)),
                       ecomm_pl:setv(<<"Content-Length">>, CLenBin, Headers);
                   _ ->
                       Headers
               end,
    <<"SIP/2.0 ", (integer_to_binary(Code))/binary, " ", Status/binary, ?ENC_NL/binary,
      << <<K/binary, ": ",
           (if is_list(V) -> encode_header({K, V}); is_binary(V) -> V end)/binary,
           ?ENC_NL/binary>> ||
          {K, V} <- Headers1 >>/binary, ?ENC_NL/binary,
      Body/binary>>;
    
encode({sip_reply, Status, Code, Headers, MultiPartSpecs})
  when is_binary(Status), is_integer(Code), is_list(Headers), is_list(MultiPartSpecs) ->
    {[multipart, Parts], []} = ecomm_pl:mgetv([type, parts], MultiPartSpecs),
    case length(Parts) of
        1 ->
            [Part] = Parts,
            {[PartHeaders, PartContent], []} = ecomm_pl:mgetv([headers, content], Part),
            {_, PartCType} = lists:keyfind(<<"Content-Type">>, 1, PartHeaders), % using just this one
            Headers1 = ecomm_pl:setv(<<"Content-Type">>, PartCType, Headers),
            encode({sip_reply, Status, Code, Headers1, PartContent});
        N when N > 1 ->
            Body = << (<< <<(encode_multipart(MP))/binary>> || MP <- Parts >>)/binary,
                      "--", ?ENC_MIME_BOUNDARY/binary, "--", ?ENC_NL/binary>>,
            CType = <<"multipart/mixed;boundary=", ?ENC_MIME_BOUNDARY/binary>>,
            Headers1 = ecomm_pl:setv(<<"Content-Type">>, CType, Headers),
            encode({sip_reply, Status, Code, Headers1, Body})
    end.

encode_multipart(MultiPartSpec) when is_list(MultiPartSpec) ->
    {[Headers, Content], []} = ecomm_pl:mgetv([headers, content], MultiPartSpec),
    {_, ContentType} = lists:keyfind(<<"Content-Type">>, 1, Headers),
    Headers1 = proplists:delete(<<"Content-Type">>, Headers),
    <<"--", ?ENC_MIME_BOUNDARY/binary, ?ENC_NL/binary, 
      "Content-Type: ", ContentType/binary, ?ENC_NL/binary,
      << <<K/binary, ": ", V/binary, ?ENC_NL/binary >> || {K, V} <- Headers1 >>/binary, ?ENC_NL/binary, 
      Content/binary, ?ENC_BL/binary>>.

%%

encode_header({<<"Contact">>, ContactKVs}) ->
    encode_address(ContactKVs);
encode_header({<<"From">>, AddrKVs}) ->
    encode_address(AddrKVs);
encode_header({<<"To">>, AddrKVs}) ->
    encode_address(AddrKVs).


encode_parameters([]) ->
    <<>>;
encode_parameters([X | Xs]) ->
    KVBin = fun ({K, V}) -> <<(to_bin(K))/binary, "=", (to_bin(V))/binary>> end,
    lists:foldl(fun ({K, V}, Acc) -> <<Acc/binary, ";", (KVBin({K, V}))/binary>> end, KVBin(X), Xs).


encode_parameters_suffix([]) ->
    <<>>;
encode_parameters_suffix([_ | _] = KVs) ->
    <<";", (encode_parameters(KVs))/binary>>.

encode_address([_ | _] = Props) ->
    Prefix = case proplists:get_value(display_name, Props) of
                 undefined -> <<>>;
                 Name -> <<(to_bin(Name))/binary, " ">>
             end,
    {InsideParamKVs, TagParamKVs} = lists:partition(fun ({"tag", _}) -> false; ({_, _}) -> true end, 
                                                    proplists:get_value(parameters, Props, [])),
    InsideParams = encode_parameters_suffix(InsideParamKVs),
    AddressPrefix = case ecomm_pl:mgetv([scheme, service, target], Props) of
                        {[Scheme, Service, undefined], [target]} ->
                            <<(to_bin(Scheme))/binary, ":service:", (to_bin(Service))/binary>>;
                        {[Scheme, undefined, Target], [service]} ->
                            <<(to_bin(Scheme))/binary, ":", (to_bin(Target))/binary>>
                    end,
    HostPort = case ecomm_pl:mgetv([host, port], Props) of
                   {[Host, Port], []} ->
                       <<"@", (to_bin(Host))/binary, ":", (to_bin(Port))/binary>>;
                   {[Host, undefined], [port]} ->
                       <<"@", (to_bin(Host))/binary>>;
                   {[undefined, undefined], [host, port]} ->
                       <<>>
               end,
    Address = <<"<", AddressPrefix/binary, HostPort/binary, InsideParams/binary, ">">>,
    OutsideParams = encode_parameters_suffix(TagParamKVs),
    <<Prefix/binary, Address/binary, OutsideParams/binary>>.


%%

to_bin(S) when is_list(S) -> list_to_binary(S);
to_bin(I) when is_integer(I) -> integer_to_binary(I);
to_bin(F) when is_float(F) -> float_to_binary(F);
to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(B) when is_binary(B) -> B.

