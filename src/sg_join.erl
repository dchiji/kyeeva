%%    Copyright 2009~2010  CHIJIWA Daiki <daiki41@gmail.com>
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

-module(sg_join).

-export([process_0/6,
        process_1/6]).

-include("../include/common_sg.hrl").

-define(TIMEOUT, infinity).


%%--------------------------------------------------------------------
%%  join process 0
%%--------------------------------------------------------------------
%% handle_call<join-process-0>から呼び出される．
%% 最もNewKeyに近いピアを(内側に向かって)探索する．
process_0(MyKey, From, Server, NewKey, MVector, Level) ->
    case ets:lookup(incomplete_table, MyKey) of
        [{MyKey, {join, -1}, {remove, -1}}] ->
            %% join-level will being updated
            timer:sleep(200),
            gen_server:call(?SERVER_MODULE, {MyKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level});
        [{MyKey, {join, _}, {remove, RLevel}}] when RLevel /= -1 ->
            %% MyKey is being removed
            %% TODO
            pass;
        _ ->
            process_0_1(MyKey, From, Server, NewKey, MVector, Level)
    end.

process_0_1(MyKey, From, Server, NewKey, MVector, Level) when MyKey == NewKey ->
    [{MyKey, MyPstate}] = ets:lookup(peer_table, MyKey),
    case lists:nth(Level - 1, MyPstate#pstate.smaller) of
        {nil, nil}          -> gen_server:reply(From, {error, mismatch});
        {BestServer, BestKey} -> gen_server:call(BestServer, {BestKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level})
    end;
process_0_1(MyKey, From, Server, NewKey, MVector, Level) ->
    [{MyKey, MyPstate}] = ets:lookup(peer_table, MyKey),
    {Neighbor, S_or_B} = if
        NewKey < MyKey -> {MyPstate#pstate.smaller, smaller};
        MyKey < NewKey -> {MyPstate#pstate.bigger, bigger}
    end,
    process_0_2(MyKey, From, Server, NewKey, MVector, Level, Neighbor, S_or_B).

process_0_2(MyKey, From, Server, {KeyN, _}=NewKey, MVector, 1=Level, Neighbor, S_or_B) ->
    case util:select_best(Neighbor, KeyN, S_or_B) of
        {nil, nil} ->
            %% go to next step (join_process_1)
            %% because I lock other process of MyKey, I use not gen_server:handle_call but function-call
            process_1(MyKey, From, Server, NewKey, MVector, Level);
        {self, self} ->
            %% go to next step (join_process_1)
            %% because I lock other process of MyKey, I use not gen_server:handle_call but function-call
            process_1(MyKey, From, Server, NewKey, MVector, Level);
        {BestServer, BestKey} ->
            %% select the best peer and go on join_process_0
            gen_server:call(BestServer, {BestKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level})
    end;
process_0_2(MyKey, From, Server, NewKey, MVector, Level, Neighbor, S_or_B) ->
    NextPeer = case ets:lookup(incomplete_table, MyKey) of
        [{MyKey, {join, _}, {remove, RLevel}}] when RLevel /= -1 ->
            %% MyKey is being removed
            %% TODO
            pass;
        [{MyKey, {join, MaxLevel}, {remove, _}}] when MaxLevel + 1 < Level ->
            %% it never match this
            error;
        _ -> lists:nth(Level - 1, Neighbor)
    end,
    process_0_3(MyKey, From, Server, NewKey, MVector, Level, Neighbor, S_or_B, NextPeer).

% Neighbor[Level]がNewKey => MyKeyはNewKeyの隣のピア
process_0_3(MyKey, From, Server, NewKey, MVector, Level, _, _, {_, NextKey}) when NewKey == NextKey ->
    process_1(MyKey, From, Server, NewKey, MVector, Level);
% Level(N - 1)以上のNeighborを対象にすることで，無駄なメッセージング処理を無くす
process_0_3(MyKey, From, Server, {KeyN, _}=NewKey, MVector, Level, Neighbor, S_or_B, _) ->
    BestPeer = case ets:lookup(incomplete_table, MyKey) of
        [{MyKey, {join, MaxLevel}}] when MaxLevel + 1 == Level -> util:select_best(lists:nthtail(MaxLevel, Neighbor), KeyN, S_or_B);
        [{MyKey, {join, MaxLevel}}] when MaxLevel < Level      -> error;    %% it never match this
        _                                                      -> util:select_best(lists:nthtail(Level - 1, Neighbor), KeyN, S_or_B)
    end,
    case BestPeer of
        {nil, nil}          -> process_1(MyKey, From, Server, NewKey, MVector, Level);
        {self, self}        -> process_1(MyKey, From, Server, NewKey, MVector, Level);
        {BestServer, BestKey} -> gen_server:call(BestServer, {BestKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level})
    end.


%%--------------------------------------------------------------------
%%  join process 1
%%--------------------------------------------------------------------
%% process_0/3関数から呼び出される．
%% MVector[Level]が一致するピアを外側に向かって探索．
process_1(MyKey, From, Server, NewKey, MVector, Level) ->
    [{MyKey, MyPstate}] = ets:lookup(peer_table, MyKey),
    MyBit               = util_mvector:nth(Level, MyPstate#pstate.mvector),
    Bit                 = util_mvector:nth(Level, MVector),
    case Bit of
        MyBit ->
            process_1_1(MyKey, From, Server, NewKey, MVector, Level, MyPstate, ets:lookup(incomplete_table, MyKey));
        _ ->
            %% find the node which matchs a Nth bit of membership-vector
            Peer = case Level of
                1                     -> error;    %% Levelが1の時は必ずMVectorが一致するはずなのでありえない
                _ when NewKey < MyKey -> lists:nth(Level - 1, MyPstate#pstate.bigger);
                _ when NewKey > MyKey -> lists:nth(Level - 1, MyPstate#pstate.smaller)
            end,
            case Peer of
                {nil, nil}          -> gen_server:reply(From, {error, mismatch});
                {NextServer, NextKey} -> gen_server:call(NextServer, {NextKey, {'join-process-1', {From, Server, NewKey, MVector}}, Level})
            end
    end.

% ピアのNeighborが完成したらjoin-process-0から再開する
process_1_1(MyKey, From, Server, NewKey, MVector, Level, MyPstate, [{MyKey, {join, MaxLevel}, {remove, -1}}]) when MaxLevel < Level ->
    Neighbor = if
        NewKey < MyKey -> MyPstate#pstate.bigger;
        NewKey > MyKey -> MyPstate#pstate.smaller
    end,
    case lists:nth(MaxLevel, Neighbor) of
        {nil, nil} -> gen_server:reply(From, {error, mismatch});
        _ ->
            util_lock:wakeup_register({MyKey, MaxLevel}, self()),
            %% check incomplete_table
            case ets:lookup(incomplete_table, MyKey) of
                [{MyKey, {join, MaxLevel}, {remove, -1}}] ->
                    receive
                        getup           -> gen_server:call(?SERVER_MODULE, {MyKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level});
                        {error, Reason} -> io:format("wakeup error: ~p~n", [Reason])
                    end;
                _ -> process_1_1(MyKey, From, Server, NewKey, MVector, Level, MyPstate, ets:lookup(incomplete_table, MyKey))
            end
    end;
% util_lock:set_neighbor処理を行い，反対側のピアにもメッセージを送信する
process_1_1(MyKey, From, Server, NewKey, _MVector, Level, MyPstate, _) ->
    Ref = make_ref(),
    {Neighbor, Reply} = if
        NewKey < MyKey -> {MyPstate#pstate.smaller, {ok, {{nil, nil}, {whereis(?SERVER_MODULE), MyKey}}, {self(), Ref}}};
        MyKey < NewKey -> {MyPstate#pstate.bigger, {ok, {{whereis(?SERVER_MODULE), MyKey}, {nil, nil}}, {self(), Ref}}}
    end,
    case lists:nth(Level, Neighbor) of
        {nil, nil} ->
            gen_server:reply(From, Reply),
            wait_remote_update(Server, fun() -> util_lock:set_neighbor(MyKey, {Server, NewKey}, Level) end, Ref);
        {OtherServer, OtherKey} ->
            MyServer = whereis(?SERVER_MODULE),
            UpdateCallback = case OtherServer of
                MyServer ->
                    %% if OtherServer is myself, i call util_lock:set_neighbor directly
                    fun() ->
                            util_lock:set_neighbor(MyKey, {Server, NewKey}, Level),
                            util_lock:set_neighbor(OtherKey, {Server, NewKey}, Level)
                    end;
                _ ->
                    fun() ->
                            {ok, Ref1, Pid} = gen_server:call(OtherServer, {OtherKey, {'join-process-2', {Server, NewKey}}, Level, {whereis(?SERVER_MODULE), MyKey}}),
                            util_lock:set_neighbor(MyKey, {Server, NewKey}, Level),
                            Pid ! {ok, Ref1}
                    end
            end,
            if
                NewKey < MyKey -> gen_server:reply(From, {ok, {{OtherServer, OtherKey}, {MyServer, MyKey}}, {self(), Ref}});
                NewKey > MyKey -> gen_server:reply(From, {ok, {{MyServer, MyKey}, {OtherServer, OtherKey}}, {self(), Ref}})
            end,
            wait_remote_update(Server, UpdateCallback, Ref)
    end.

% reply先がNeighborの更新に成功するまで待機
wait_remote_update(Server, UpdateCallback, Ref) ->
    receive
        {ok, Ref} ->
            UpdateCallback(),
            case ets:lookup(node_ets_table, Server) of
                [] -> spawn(fun() -> ets:insert(node_ets_table, {Server, gen_server:call(Server, {'get-ets-table'})}) end);
                _  -> ok
            end;
        {error, Ref, Reason} -> io:format("reason: ~p~n", [Reason])
    end.
