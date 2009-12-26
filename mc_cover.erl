-module(mc_cover).
-export([start/0, init/1]).
-behaviour(supervisor).

-define(PORT, 5678).


start() ->
    supervisor:start_link({local, ?MODULE}, spd, []).


init(_Args) ->
    RestartStrategy = one_for_one,
    MaxR = 1,
    MaxT = 60,
    ChildSpec = [childspec()],

    {ok, {{RestartStrategy, MaxR, MaxT}, ChildSpec}}.


childspec() ->
    childspec({?MODULE, mc_cover, []}).

childspec(StartFunc) ->
    ID = ?MODULE,
    Restart = permanent,
    Shutdown = transient,
    Type = worker,
    Modules = [?MODULE],

    {ID, StartFunc, Restart, Shutdown, Type, Modules}.


mc_cover() ->
    {ok, LSock} = gen_tcp:listen(
        ?PORT,
        [binary,
            {packet, line},
            {active, false},
            {reuseaddr, true}]),

    accept_loop(LSock).


accept_loop(LSock) ->
    {ok, Sock} = gen_tcp:accept(LSock),

    ChildSpec = childspec({?MODULE, recv_loop, [Sock]}),
    supervisor:start_child(?MODULE, ChildSpec),

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
                            %gen_tcp:send(
                            %    Sock,
                            %    io_lib:format("VALUE ~s 0 ~w¥r¥n~s¥r¥nEND¥r¥n",
                            %        [Key, size(Value), Value]))
                            pass
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
