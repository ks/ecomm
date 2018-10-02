-module(ecomm_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([map_tree/2]).

%%%%%%%%%%

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, 
          [{ecomm_conn_mgr_sup, 
            {ecomm_conn_mgr_sup, start_link, []},
            permanent, 5000, worker, [ecomm_conn_mgr_sup]}]}}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

map_tree(Fn, TopSup) ->
    {TopSup,
     [case Type of
          worker ->
              {Type, Pid, Fn(Kid)};
          supervisor ->
              {Type, Pid, Fn(Kid), map_tree(Fn, Pid)}
      end || {_Name, Pid, Type, _Mods} = Kid <- supervisor:which_children(TopSup)]}.
