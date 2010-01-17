-module(test).
-export([test/0]).
-export([mv_test/0, join_test/2]).


test() ->
    {ok, Server} = skipgraph:start('__none__'),

    skipgraph:join(0),
    join_test(),

    timer:sleep(30000),

    skipgraph:remove(10),
    io:format("get(10)=~p~n", [skipgraph:get(10)]),

    timer:sleep(infinity).

join_test() ->
    join_test(1, 1000).

join_test(N, N) ->
    ok;
join_test(N, M) ->
    spawn(fun() -> skipgraph:join(N) end),
    timer:sleep(10),
    join_test(N + 1, M).


get_test() ->
    A = skipgraph:get(13),
    B = skipgraph:get(30, 50),
    C = skipgraph:get(0, 20),
    {A, B, C}.


put_test() ->
    skipgraph:join(93, value).


mv_test() ->
    lists:map(fun(_) -> mv() end, lists:duplicate(30, 0)).

mv() ->
    mv(skipgraph:make_membership_vector(), 32 - 1),
    io:format("~n").

mv(MV, -1) ->
    ok;
mv(MV, N) ->
    M = 32 - N - 1,
    <<_:M, Bit:1, Tail:N>> = MV,
    io:format("~p ", [Bit]),
    mv(MV, N - 1).

