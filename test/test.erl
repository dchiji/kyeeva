-module(test).
-compile(export_all).

test() ->
    {ok, Server} = sg_server:start(nil),

    sg_server:put(0, [{type, 0}]),
    join_test(),

    timer:sleep(infinity),

    sg_server:remove(10),
    io:format("get(10)=~p~n", [sg_server:get(10)]),

    timer:sleep(infinity).

join_test() ->
    join_test(1, 1000).

join_test(N, N) ->
    ok;
join_test(N, M) ->
    sg_server:put(N, [{type, N}]),
    timer:sleep(10),
    join_test(N + 1, M).


get_test() ->
    A = sg_server:get(13),
    B = sg_server:get(30, 50),
    C = sg_server:get(0, 20),
    {A, B, C}.

