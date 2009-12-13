-module(test).
-export([test/0]).

test() ->
    {ok, Server} = skipgraph:start('__none__'),
    io:format("~nResult: ~p~n", [{
                {join_test, join_test()},
                {get_test, get_test()}}]).

join_test() ->
    skipgraph:join(32),
    skipgraph:join(48),
    skipgraph:join(28),
    skipgraph:join(3),
    skipgraph:join(52),
    skipgraph:join(98),
    skipgraph:join(10),
    skipgraph:join(2),
    skipgraph:join(13),
    skipgraph:join(29),
    skipgraph:join(50),
    skipgraph:join(31),
    skipgraph:join(5),
    skipgraph:join(66),
    skipgraph:join(1),

    skipgraph:test(),
    ok.

get_test() ->
    A = skipgraph:get(13),
    B = skipgraph:get(30, 50),
    C = skipgraph:get(0, 20),
    {A, B, C}.
