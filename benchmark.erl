-module(benchmark).
-export([benchmark/0]).

benchmark() ->
    {ok, Server} = skipgraph:start('__none__'),
    skipgraph:join(0),
    join(),
    timer:sleep(infinity).

join() ->
    join(1, 1000000).

join(N, N) ->
    ok;
join(N, M) ->
    skipgraph:join(N),
    join(N + 1, M).
