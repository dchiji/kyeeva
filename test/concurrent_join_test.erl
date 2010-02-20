-module(concurrent_join_test).
-compile(export_all).

test() ->
    {ok, Server} = skipgraph:start('__none__'),

    skipgraph:put(0, [{type, 0}]),
    join_test(),

    timer:sleep(infinity),

    skipgraph:remove(10),
    io:format("get(10)=~p~n", [skipgraph:get(10)]),

    timer:sleep(infinity).

join_test() ->
    join_test(1, 1000).

join_test(N, N) ->
    ok;
join_test(N, M) ->
    spawn(fun() -> skipgraph:put(N, [{type, N}]) end),
    timer:sleep(10),
    join_test(N + 1, M).


get_test() ->
    A = skipgraph:get(13),
    B = skipgraph:get(30, 50),
    C = skipgraph:get(0, 20),
    {A, B, C}.

