-module(benchmark).
-export([benchmark/0]).

benchmark() ->
    {ok, Server} = skipgraph:start('__none__'),
    skipgraph:put(0, [{type, 0}, {random, random:uniform(100000)}]),
    join(),
    timer:sleep(infinity).

join() ->
    join(1, 1000).

join(N, N) ->
    ok;
join(N, M) ->
    skipgraph:put(N, [{random, random:uniform(100000)}, {type, N}]),
    join(N + 1, M).
