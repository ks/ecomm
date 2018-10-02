-module(ecomm_ldap).

-export([search/3, search/4, search/5, search/6]).
-export([simple_bind/3]).

-include_lib("eldap/include/eldap.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

simple_bind(Handle, BaseKVs, Password)
  when is_pid(Handle) andalso is_list(BaseKVs) andalso is_list(Password) ->
    eldap:simple_bind(Handle, search_base(BaseKVs), Password).

search(Handle, BaseKVs, Scope) ->
    search(Handle, BaseKVs, Scope, undefined, [], []).
search(Handle, BaseKVs, Scope, Filter) ->
    search(Handle, BaseKVs, Scope, Filter, [], []).
search(Handle, BaseKVs, Scope, Filter, Attributes) ->
    search(Handle, BaseKVs, Scope, Filter, Attributes, []).
search(Handle, BaseKVs, Scope, Filter, Attributes, SearchOpts)
  when is_pid(Handle) andalso is_list(BaseKVs) andalso is_atom(Scope) andalso 
       is_list(Attributes) andalso is_list(SearchOpts) andalso
       (Filter == undefined orelse is_tuple(Filter)) ->
    Search = [{base, search_base(BaseKVs)},
              {scope, eldap:Scope()},
              {filter, search_filter(Filter)} |
              case Attributes of
                  [] -> [];
                  [_ | _] -> [{attributes, Attributes}]
              end] ++ SearchOpts,
    case eldap:search(Handle, Search) of
        {ok, #eldap_search_result{entries = Entries, referrals = Referrals}} ->
            Entries1 = [{parse_assignment_seq(DNString, ",", "="), ResAttrs} ||
                           #eldap_entry{object_name = DNString, attributes = ResAttrs} <- Entries],
            {ok, Entries1, Referrals};
        {error, Reason} ->
            {error, Reason}
    end.


search_base(BaseKVs) ->
    string:join([ecomm_str:fmt("~s=~s", [K, V]) || {K, V} <- BaseKVs], ",").


%% example filter:
%%
%% ecomm_ldap:search_filter(
%%   {'not', {'and', [{present, "cn"}, 
%%                    {equalityMatch, "dn", "123"}, 
%%                    {'or', [{substrings, "dn", [{any, "qwer"}, {final, "wert"}]}, 
%%                            {greaterOrEqual, "qw", 100}]}]}}).
%% ==>
%% {filter,
%%     {'not',
%%         {'and',
%%             [{present,"cn"},
%%              {equalityMatch,{'AttributeValueAssertion',"dn","123"}},
%%              {'or',
%%                  [{substrings,
%%                       {'SubstringFilter',"dn",[{any,"qwer"},{final,"wert"}]}},
%%                   {greaterOrEqual,{'AttributeValueAssertion',"qw",100}}]}]}}}

search_filter(undefined) ->
    eldap:present("objectClass");
search_filter({'not', Filter}) ->
    eldap:'not'(search_filter(Filter));
search_filter({Comb, Filters}) when (Comb == 'and' orelse Comb == 'or') andalso is_list(Filters) ->
    eldap:Comb([search_filter(F) || F <- Filters]);
search_filter(Filter) when is_tuple(Filter) ->
    [FilterFn | FilterArgs] = tuple_to_list(Filter),
    apply(eldap, FilterFn, FilterArgs).


parse_assignment_seq(String, PairDelim, KVBindSym) ->
    parse_assignment_seq(String, PairDelim, KVBindSym, fun ({K, V}) -> {K, V} end).
parse_assignment_seq(String, PairDelim, KVBindSym, KVFun) ->
    [KVFun(list_to_tuple([_, _] = string:tokens(P, KVBindSym))) || 
        P <- string:tokens(String, PairDelim)].

