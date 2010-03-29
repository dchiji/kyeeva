%%    Copyright 2009-2010  CHIJIWA Daiki <daiki41@gmail.com>
%%    
%%    Redistribution and use in source and binary forms, with or without
%%    modification, are permitted provided that the following conditions
%%    are met:
%%    
%%         1. Redistributions of source code must retain the above copyright
%%            notice, this list of conditions and the following disclaimer.
%%    
%%         2. Redistributions in binary form must reproduce the above copyright
%%            notice, this list of conditions and the following disclaimer in
%%            the documentation and/or other materials provided with the
%%            distribution.
%%    
%%    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%%    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%%    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
%%    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE FREEBSD
%%    PROJECT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%%    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
%%    TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
%%    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
%%    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
%%    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
%%    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-module(mcfe).
-export([start/0, init/1]).
-behaviour(supervisor).

-define(PORT, 5678).


start() ->
    supervisor:start_link({local, ?MODULE}, spd, []).


%%====================================================================
%% supervisor callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init
%% Description: supervisor:start_linkにより呼び出される．
%% Returns: {ok,{{RestartStrategy,MaxR,MaxT},[ChildSpec]}}
%%--------------------------------------------------------------------
init(_Args) ->
    RestartStrategy = one_for_one,
    MaxR = 1,
    MaxT = 60,
    ChildSpec = [childspec()],

    {ok, {{RestartStrategy, MaxR, MaxT}, ChildSpec}}.


%%--------------------------------------------------------------------
%% Function: childspec
%% Description: 子プロセスの詳細設定を返す
%% Returns: {Id,StartFunc,Restart,Shutdown,Type,Modules}
%%--------------------------------------------------------------------
childspec() ->
    childspec({?MODULE, mc_cover, []}).

childspec(StartFunc) ->
    ID = ?MODULE,
    Restart = permanent,
    Shutdown = transient,
    Type = worker,
    Modules = [?MODULE],

    {ID, StartFunc, Restart, Shutdown, Type, Modules}.


%%====================================================================
%% memcached protocol
%%====================================================================

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

                ["delete", Key] ->
                    pass,
                    gen_tcp:send(Sock, "DELETED¥r¥n");

                ["get", Key] ->
                    case skipgraph:get(Key, Key) of
                        {ok, []} ->
                            gen_tcp:send(Sock, "END\r\n");

                        {ok, [{Key, Value}]} ->
                            gen_tcp:send(
                                Sock,
                                io_lib:format("VALUE ~s 0 ~w¥r¥n~s¥r¥nEND¥r¥n",
                                    [Key, size(Value), Value]))
                    end;

                ["set", Key, _Flags, _Expire, Bytes] ->
                    case gen_tcp:recv(Sock, list_to_integer(Bytes)) of
                        {ok, Value} ->
                            skipgraph:join(Key, Value),
                            gen_tcp:send(Sock, "STORED¥r¥n");

                        {error, closed} ->
                            ok
                    end
            end,
            recv_loop(Sock);

        {error, closed} ->
            ok;

        Error ->
            io:format("error: ~p~n", [Error]),
            error
    end.
