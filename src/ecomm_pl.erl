-module(ecomm_pl).

-export([getv/2, getv/3, setv/3, del/2, condv/4, mgetv/2, agetv/2, agetv/3, update/2]).
-export([merge/3, multi_merge/2]).
-export([rewrite/2, rewrite/3]).

%%%==============================================
%%% Helpers for nested proplists
%%%==============================================

% dive in to nested proplist
getv(Key, PL) -> getv(Key, PL, undefined).
getv(Key, PL, Default) when not is_list(Key) -> getv([Key], PL, Default);
getv([Key], PL, Default) -> proplists:get_value(Key, PL, Default);
getv([Key | Rest], PL, Default) ->
    case lists:keyfind(Key, 1, PL) of
        false -> Default;
        {Key, Val} when is_list(Val) ->
            case io_lib:char_list(Val) of
                true -> Default;
                _ -> getv(Rest, Val, Default)
            end;
        _ -> Default
    end.

% update nested proplist
setv(Key, Val, PL) when not is_list(Key) -> setv([Key], Val, PL);
setv([Key], Val, PL) -> lists:keystore(Key, 1, PL, {Key, Val});
setv([Key | Rest], Val, PL) ->
    CurrentProp = case lists:keyfind(Key, 1, PL) of
        {Key, Prop} when is_list(Prop) ->
            case io_lib:char_list(Prop) of true -> []; _ -> Prop end;
        _ -> []
    end,
    NKey = case io_lib:char_list(Key) of true -> [Key]; _ -> Key end,
    setv(NKey, setv(Rest, Val, CurrentProp), PL).

% delete from nested proplist
del(Key, PL) when not is_list(Key) -> del([Key], PL);
del([Key], PL) -> lists:keydelete(Key, 1, PL);
del([Key | Rest], PL) ->
    case proplists:get_value(Key, PL) of
        Prop when is_list(Prop) ->
            NKey = case io_lib:char_list(Key) of true -> [Key]; _ -> Key end,
            setv(NKey, del(Rest, Prop), PL);
        _ -> PL
    end.

% Generate a value conditionally on a proplist element, undefined is treated as
% normal value. If the value is found in the ValueMap which is a {Value, Mapped}
% mapping list then the Mapped value is returned unless Mapped is a fun in which
% case the result is fun(Value). If the value is not found in the mapping list
% then Default is returned. Again if Default is a fun then fun(Value) is returned
condv(Name, Proplist, ValueMap, Default) ->
    Value = getv(Name, Proplist),
    case lists:keyfind(Value, 1, ValueMap) of
        false when is_function(Default) -> Default(Value);
        false -> Default;
        {Value, Mapped} when is_function(Mapped)-> Mapped(Value);
        {Value, Mapped} -> Mapped
    end.

% Function to get multiple values out of proplists
% The Description can be the Name of the property,
% a 2 element tuple with name and default {Name, Default}
% If only Name is given and the property is undefined then the key is added to
% MissingKeyList
mgetv(Descriptions, Proplist) ->
    mgetv(Descriptions, Proplist, [], []).
mgetv([{Name, Default} | Rest], Proplist, Acc, ErrAcc) ->
    mgetv(Rest , Proplist, [getv(Name, Proplist, Default) | Acc], ErrAcc);
mgetv([Name | Rest], Proplist, Acc, ErrAcc) ->
    case getv(Name, Proplist) of
        undefined ->
            mgetv(Rest , Proplist, [undefined | Acc], [Name | ErrAcc]);
        Value ->
            mgetv(Rest , Proplist, [Value | Acc], ErrAcc)
    end;
mgetv([], _, Acc, ErrAcc) -> {lists:reverse(Acc), lists:reverse(ErrAcc)}.


% Alternative getv. If a certain parameter can be in different places in the struct
% take the list of positions and return the first that is not undef.
agetv(Places, Proplist) -> agetv(Places, Proplist, undefined).
agetv([Place | Rest], Proplist, Default) ->
    case getv(Place, Proplist, Default) of
        undefined -> agetv(Rest, Proplist, Default);
        Value -> {Value, Place}
    end;
agetv([], _, _) -> {undefined, undefined}.

% Update multiple entries in a proplist
update([{_Key, undefined} |RestOfUpdates], Data) ->
    update(RestOfUpdates, Data);
update([{Key, Value} |RestOfUpdates], Data) ->
    update(RestOfUpdates, setv(Key, Value, Data));
update([], Result) -> Result.


%%%%%%%%%%

is_tree_node(Tree) ->
    is_list(Tree) andalso lists:all(fun ({_,_}) -> true; (_) -> false end, Tree).

split_tree_node(Tree, K) ->
    case lists:splitwith(fun ({KK, _}) -> K /= KK; (_) -> true end, Tree) of
        {P, [{K, _} | S]} -> {P, S};
        {P, S} -> {P, S}
    end.

merge(Tree, {Ks, Val}, combine) ->
    is_tree_node(Val) orelse error(cant_combine),
    merge1(Tree, {Ks, Val}, combine);
merge(Tree, {Ks, Val}, replace) ->
    merge1(Tree, {Ks, Val}, replace).

merge1(Tree, {[K0, K1 | Ks], Val}, Mode) ->
    {Prefix, Suffix} = split_tree_node(Tree, K0),
    Prefix ++ [{K0, merge1(proplists:get_value(K0, Tree, []), {[K1 | Ks], Val}, Mode)}] ++ Suffix;
merge1(Tree, {[K], Val}, combine) ->
    case lists:keyfind(K, 1, Tree) of
        {K, PrevVal} ->
            case is_tree_node(PrevVal) of
                true ->
                    {Prefix, Suffix} = split_tree_node(Tree, K),
                    Combined = lists:foldl(fun ({K1, V1}, Acc) ->
                                                   lists:keystore(K1, 1, Acc, {K1, V1})
                                           end, PrevVal, Val),
                    Prefix ++ [{K, Combined}] ++ Suffix;
                false ->
                    error(cant_combine)
            end;
        false ->
            Tree ++ [{K, Val}]
    end;
merge1(Tree, {[K], Val}, replace) ->
    lists:keystore(K, 1, Tree, {K, Val}).

multi_merge(Tree, []) ->
    Tree;
multi_merge(Tree, [{{Path, Vals}, Mode} | RemSpecs]) ->
    multi_merge(merge(Tree, {Path, Vals}, Mode), RemSpecs).


%%%%%%%%%%

rw({{prefix_path, Ks}, CurrPath, CWD, T}) ->
    {ok, F} = prefix_path(CurrPath, CWD),
    ecomm_pl:setv(Ks, F, T);
rw({{full_path, Ks}, CurrPath, CWD, T}) ->
    {ok, F} = full_path(CurrPath, CWD),
    ecomm_pl:setv(Ks, F, T);
rw({{set_cwd, Ks}, _, CWD, T}) ->
    ecomm_pl:setv(Ks, CWD, T);
rw({{set, Ks, V}, _, _CWD, T}) ->
    ecomm_pl:setv(Ks, V, T);
rw({{del, Ks}, _, _CWD, T}) ->
    ecomm_pl:del(Ks, T).

maybe_do_pathname(CurrPath, ("/" ++ _) = Dir, Callback) ->
    case lists:prefix(Dir, CurrPath) of
        true -> {ok, CurrPath};
        false -> Callback({CurrPath, Dir})
    end.

prefix_path(CurrPath, Dir) ->
    maybe_do_pathname(CurrPath, Dir, fun (_) -> {ok, filename:join(Dir, CurrPath)} end).
full_path(CurrPath, Dir) ->
    maybe_do_pathname(CurrPath, Dir, fun (_) -> full_path1(filename:split(CurrPath), Dir, []) end).
full_path1([], _Dir, []) ->
    {error, not_found};
full_path1([_ | SubDirs] = PathChunks, Dir, []) ->
    R = filelib:wildcard("**/" ++ filename:join(PathChunks), Dir),
    case R of
        [Res] -> {ok, filename:join(Dir, Res)};
        _ -> full_path1(SubDirs, Dir, [])
    end.


rewrite(RWs, Tree) ->
    {ok, CWD} = file:get_cwd(),
    rewrite(RWs, CWD, Tree).
rewrite(RWs, CWD, Tree) ->
    lists:foldl(fun (Spec, T) -> 
                        rw({Spec, ecomm_pl:getv(element(2, Spec), T), CWD, T}) 
                end, Tree, RWs).
   
