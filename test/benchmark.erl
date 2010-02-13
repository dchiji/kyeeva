-module(benchmark).
-compile(export_all).

-define(GET_N, 1).
-define(MIN, 0).
-define(MAX, 100).

benchmark() ->
    {ok, Server} = skipgraph:start('__none__'),
    put_test(),
    skipgraph:test(),

    %eprof(),
    fprof(Server),
    %cprof(),
    %io:format("~p~n", [timer:tc(benchmark, get_test, [])]),

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
    Key0 = random:uniform(?MAX),
    Key1 = random:uniform(?MAX),
    io:format("~n~n~nKey0=~p Key1=~p~n", [Key0, Key1]),
    io:format("result=~p~n", [skipgraph:get({type, Key0}, {type, Key1}, [type])]),
    %skipgraph:get({type, random:uniform(?MAX)}, [type]),
    get_test(N - 1).


eprof() ->
    eprof:start(),
    eprof:profile([], ?MODULE, get_test, []),
    eprof:analyse(),
    eprof:stop().


fprof(Server) ->
    fprof:start(),
    %fprof:apply(?MODULE, get_test, []),

    fprof:trace([start, {procs, [Server]}]),
    get_test(),
    fprof:trace([stop]),

    fprof:profile(),
    fprof:analyse(),
    fprof:stop().

cprof() ->
    cprof:start(),
    get_test(),
    cprof:pause(),
    io:format("~n~n~p~n", [cprof:analyse()]),
    cprof:stop().

