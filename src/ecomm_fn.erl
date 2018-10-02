-module(ecomm_fn).

-export([identity/1,
         ensure/2,
         apply/2,
         apply_tm/3,
         pmap/2,
         pmap/4]).

%% internal
-export([do_apply_fn/4]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

identity(X) -> X.
     
ensure(F, Arity) when is_function(F, Arity) -> 
    F;
ensure({M, F}, Arity) when is_atom(M), is_atom(F) ->
    true = lists:member({F, Arity}, M:module_info(exports)),
    {M, F}.

apply({M, F}, Args) -> erlang:apply(M, F, Args);
apply(F, Args) -> erlang:apply(F, Args).


apply_tm(Fn, Args, infinity) ->
    ?MODULE:apply(Fn, Args);
apply_tm(Fn, Args, TimeoutMS) when TimeoutMS >= 0 ->
    Caller = self(),
    Ref = make_ref(),
    Pid = spawn(?MODULE, do_apply_fn, [Caller, Ref, Fn, Args]),
    receive
        {ecomm_fn_reply, Ref, Res} -> Res
    after 
        TimeoutMS ->
            erlang:exit(Pid, kill),
            {error, timeout}
    end.

do_apply_fn(Caller, Ref, Fn, Args) ->
    Caller ! {ecomm_fn_reply, Ref, catch ?MODULE:apply(Fn, Args)}.

%%%%%%%%%%

%% built-in rpc:pmap is rather crappy
pmap(Fn, Elts) when is_list(Elts) ->
    pmap(Fn, [], Elts, infinity).
pmap(Fn, ExtraArgs, Elts, Timeout) when is_list(ExtraArgs), is_list(Elts) ->
    Ref = make_ref(),
    Top = self(),
    Worker = spawn_link(
               fun () ->
                       WorkerRef = make_ref(),
                       WorkerTop = self(),
                       Num = lists:foldl(
                               fun (Elt, N) ->
                                       spawn_link(
                                         fun () ->
                                                 Res = ?MODULE:apply(Fn, [Elt | ExtraArgs]),
                                                 WorkerTop ! {WorkerRef, {N, Res}}
                                         end),
                                       N + 1
                               end, 0, Elts),
                       Rcv = fun Rcv(0, Acc) ->
                                     [X || {_, X} <- lists:keysort(1, Acc)];
                                 Rcv(Todo, Acc) ->
                                     receive
                                         {WorkerRef, {N, Res}} ->
                                             Rcv(Todo - 1, [{N, Res} | Acc])
                                     end
                             end,
                       Top ! {Ref, Rcv(Num, [])}
               end),
    receive 
        {Ref, Result} ->
            Result
    after
        Timeout ->
            exit(Worker, normal),
            timeout
    end.
