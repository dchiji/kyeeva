-module(mc_cover).
-export([start/0]).

-define(PORT, 5678).


start() ->
    {ok, LSock} = gen_tcp:listen(
        ?PORT,
        [binary,
            {packet, line},
            {active, false},
            {reuseaddr, true}]),

    spawn(fun() -> loop(LSock) end).


accept_loop(LSock) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    
    spawn(fun() -> recv_loop(Sock) end),
    accept_loop(LSock).


recv_loop(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Line} ->
            Tokens = string:tokens(binary_to_list(Line), " \r\n"),

            case Tokens of
                ["get", "range", Key0, Key1] ->
                    case skipgraph:get(Key0, Key1) of
                        {ok, []} ->
                            gen_tcp:send(Sock, "END\r\n");

                        {ok, Items} ->
                            gen_tcp:send(
                                Sock,
                                %io_lib:format("VALUE ~s 0 ~w¥r¥n~s¥r¥nEND¥r¥n",
                                %    [Key, size(Value), Value]))
                    end;

                ["get", Key] ->
                    case skipgraph:get(Key, Key) of
                        {ok, []} ->
                            gen_tcp:send(Sock, "END\r\n");

                        {ok, [{Key, Value}]} ->
                            gen_tcp:send(
                                Sock,
                                io_lib:format("VALUE ~s 0 ~w¥r¥n~s¥r¥nEND¥r¥n",
                                    [Key, size(Value), Value]))
                    end
            end,
            recv_loop(Sock);

        {error, closed} ->
            ok;

        Error ->
            io:format("error: ~p~n", [Error]),
            error
    end.
