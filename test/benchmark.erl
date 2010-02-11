-module(benchmark).
-compile(export_all).

-define(GET_N, 1000).
-define(MIN, 0).
-define(MAX, 100).

benchmark() ->
    {ok, Server} = skipgraph:start('__none__'),

    put_test(),
    prof(Server),
%    io:format("~p~n", [timer:tc(benchmark, get_test, [])]),

    timer:sleep(infinity).


put_test() ->
    put_test(?MIN, ?MAX).

put_test(N, N) ->
    ok;
put_test(N, M) ->
    %skipgraph:put(N, [{random, random:uniform(100000)}, {type, N}]),
    skipgraph:put(N, [{type, N}]),
    put_test(N + 1, M).


get_test() ->
    get_test(?GET_N).

get_test(0) ->
    true;
get_test(N) ->
    %skipgraph:get({type, random:uniform(?MAX)}, {type, random:uniform(?MAX)}, [type]),
    skipgraph:get({type, random:uniform(?MAX)}, [type]),
    get_test(N - 1).


prof(Server) ->
    fprof:start(),
    %fprof:apply(?MODULE, get_test, []),

    fprof:trace([start, {procs, [Server]}]),
    get_test(),
    fprof:trace([stop]),

    fprof:profile(),
    fprof:analyse(),
    fprof:stop().
