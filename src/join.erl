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

-module(join).
-export([process_0/3, process_1/3, process_0_oneway/3, process_1_oneway/3]).

%-define(LEVEL_MAX, 8).
%-define(LEVEL_MAX, 16).
-define(LEVEL_MAX, 32).
%-define(LEVEL_MAX, 64).
%-define(LEVEL_MAX, 128).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).

-define(SERVER_MODULE, skipgraph).


%% handle_call<join-process-0>から呼び出される．
%% 最もNewKeyに近いピアを(内側に向かって)探索する．
process_0(SelfKey, {From, _Server, NewKey, _MembershipVector}, Level)
when Level == 0 andalso SelfKey == NewKey ->
    gen_server:reply(From, {exist, {whereis(?SERVER_MODULE), SelfKey}});

process_0(SelfKey, {From, Server, NewKey, MembershipVector}, Level)
when Level > 0 andalso SelfKey == NewKey ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?SERVER_MODULE,
                            {SelfKey,
                                {'join-process-0',
                                    {From,
                                        Server,
                                        NewKey,
                                        MembershipVector}},
                                Level})
                end);

        _ ->
            [{SelfKey, {_, _, {Smaller, _}}}] = ets:lookup('Peer', SelfKey),

            case lists:nth(Level, Smaller) of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {error, mismatch});

                {BestNode, BestKey} ->
                    spawn(fun() ->
                                gen_server:call(BestNode,
                                    {BestKey,
                                        {'join-process-0',
                                            {From,
                                                Server,
                                                NewKey,
                                                MembershipVector}},
                                        Level})
                        end)
            end
    end;

process_0(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?SERVER_MODULE,
                            {SelfKey,
                                {'join-process-0',
                                    {From,
                                        Server,
                                        NewKey,
                                        MembershipVector}},
                                Level})
                end);

        _ ->
            [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

            {Neighbor, S_or_B} = if
                NewKey < SelfKey ->
                    {Smaller, smaller};
                SelfKey < NewKey ->
                    {Bigger, bigger}
            end,

            case Level of
                0 ->
                    case select_best(Neighbor, NewKey, S_or_B) of
                        % 最適なピアが見つかったので、次のフェーズ(process_1)へ移行．
                        % 他の処理をlockしておくために関数として(process_1フェーズへ)移行する．
                        {'__none__', '__none__'} ->
                            process_1(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);
                        {'__self__'} ->
                            process_1(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);


                        % 最適なピアの探索を続ける(join-process-0)
                        {BestNode, BestKey} ->
                            spawn(fun() ->
                                        gen_server:call(BestNode,
                                            {BestKey,
                                                {'join-process-0',
                                                    {From,
                                                        Server,
                                                        NewKey,
                                                        MembershipVector}},
                                                Level})
                                end)
                    end;

                % LevelN(N > 0)の場合，Level(N - 1)以下にはNewKeyピアが存在するので，特別な処理が必要
                _ ->
                    Peer = case ets:lookup('Incomplete', SelfKey) of
                        [{SelfKey, {join, MaxLevel}}] when MaxLevel + 1 == Level ->
                            lists:nth(MaxLevel + 1, Neighbor);
                        [{SelfKey, {join, MaxLevel}}] when MaxLevel < Level ->
                            % バグが無ければ，このパターンになることはない
                            error;
                        _ ->
                            lists:nth(Level, Neighbor)
                    end,

                    case Peer of
                        % Neighbor[Level]がNewKey => SelfKeyはNewKeyの隣のピア
                        {_, NewKey} ->
                            process_1(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);

                        _ ->
                            % Level(N - 1)以上のNeighborを対象にすることで，無駄なメッセージング処理を無くす
                            BestPeer = case ets:lookup('Incomplete', SelfKey) of
                                [{SelfKey, {join, MaxLevel_}}] when (MaxLevel_ + 1) == Level ->
                                    select_best(lists:nthtail(MaxLevel_, Neighbor), NewKey, S_or_B);
                                [{SelfKey, {join, MaxLevel_}}] when MaxLevel_ < Level ->
                                    % バグが無ければ，このパターンになることはない
                                    error;
                                _ ->
                                    select_best(lists:nthtail(Level - 1, Neighbor), NewKey, S_or_B)
                            end,
                            %io:format("BestKey=~p~n", [BestKey]),

                            case BestPeer of
                                {'__none__', '__none__'} ->
                                    process_1(SelfKey,
                                        {From,
                                            Server,
                                            NewKey,
                                            MembershipVector},
                                        Level);
                                {'__self__'} ->
                                    process_1(SelfKey,
                                        {From,
                                            Server,
                                            NewKey,
                                            MembershipVector},
                                        Level);


                                % 最適なピアの探索を続ける(join-process-0)
                                {BestNode, BestKey} ->
                                    spawn(fun() ->
                                                gen_server:call(BestNode,
                                                    {BestKey,
                                                        {'join-process-0',
                                                            {From,
                                                                Server,
                                                                NewKey,
                                                                MembershipVector}},
                                                        Level})
                                        end)
                            end
                    end
            end
    end.


%% process_0/3関数から呼び出される．
%% MembershipVector[Level]が一致するピアを外側に向かって探索．
%% 成功したら自身のNeighborをupdateし，Anotherにもupdateメッセージを送信する．
process_1(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    io:format("join:process_1: SelfKey=~p, NewKey=~p, Level=~p~n", [SelfKey, NewKey, Level]),
    [{SelfKey, {_, SelfMembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    TailN = ?LEVEL_MAX - Level - 1,
    <<_:TailN, Bit:1, _:Level>> = MembershipVector,
    <<_:TailN, SelfBit:1, _:Level>> = SelfMembershipVector,

    case Bit of
        % MembershipVector[Level]が一致するのでupdate処理を行う
        SelfBit ->
            case ets:lookup('Incomplete', SelfKey) of
                % ピアのNeighborがまだ未完成な場合，強制的に次のノードへ移る
                [{SelfKey, {join, MaxLevel}}] when MaxLevel < Level ->
                    Neighbor = if
                        NewKey < SelfKey ->
                            Bigger;
                        SelfKey < NewKey ->
                            Smaller
                    end,

                    case lists:nth(MaxLevel + 1, Neighbor) of
                        {'__none__', '__none__'} ->
                            gen_server:reply(From, {error, mismatch});

                        %{OtherNode, OtherKey} ->
                        %    spawn(fun() ->
                        %            gen_server:call(OtherNode,
                        %                {OtherKey,
                        %                    {'join-process-1',
                        %                        {From,
                        %                            Server,
                        %                            NewKey,
                        %                            MembershipVector}},
                        %                Level})
                        %    end)
                        _ ->
                            io:format("~n~n__incomplete_error__ SelfKey=~p, NewKey=~p, Level=~p~n~n", [SelfKey, NewKey, Level]),
                            [{{SelfKey, MaxLevel}, Daemon}] = ets:lookup('Joining-Wait', {SelfKey, MaxLevel}),
 
                            Ref = make_ref(),
                            Pid = spawn(fun() ->
                                        receive
                                            {ok, Ref} ->
                                                gen_server:call(?SERVER_MODULE,
                                                    {SelfKey,
                                                        {'join-process-0',
                                                            {From,
                                                                Server,
                                                                NewKey,
                                                                MembershipVector}},
                                                        Level});

                                            {error, Ref, Message} ->
                                                pass
                                        end
                                end),

                            Daemon ! {add, {Pid, Ref}}
                    end;

                % update処理を行い，反対側のピアにもメッセージを送信する
                _ ->
                    Ref = make_ref(),

                    {Neighbor, Reply} = if
                        NewKey < SelfKey ->
                            {Smaller, {ok, {{'__none__', '__none__'}, {whereis(?SERVER_MODULE), SelfKey}}, {self(), Ref}}};
                        SelfKey < NewKey ->
                            {Bigger, {ok, {{whereis(?SERVER_MODULE), SelfKey}, {'__none__', '__none__'}}, {self(), Ref}}}
                    end,

                    case lists:nth(Level + 1, Neighbor) of
                        {'__none__', '__none__'} ->
                            io:format("update0, SelfKey=~p, OtherKey=~p, NewKey=~p~n", [SelfKey, {'__none__', '__none__'}, NewKey]),

                            gen_server:reply(From, Reply),

                            % reply先がNeighborの更新に成功するまで待機
                            receive
                                {ok, Ref} ->
                                    update(SelfKey, {Server, NewKey}, Level),

                                    case ets:lookup('ETS-Table', Server) of
                                        [] ->
                                            spawn(fun() ->
                                                        Tab = gen_server:call(Server, {'get-ets-table'}),
                                                        ets:insert('ETS-Table', {Server, Tab})
                                                end);
                                        _ ->
                                            ok
                                    end;

                                {error, Ref, Message} ->
                                    pass
                            end;

                        {OtherNode, OtherKey} ->
                            Self = whereis(?SERVER_MODULE),

                            F = fun(Update) ->
                                    if
                                        NewKey < SelfKey ->
                                            gen_server:reply(From, {ok, {{Self, OtherKey}, {Self, SelfKey}}, {self(), Ref}});
                                        SelfKey < NewKey ->
                                            gen_server:reply(From, {ok, {{Self, SelfKey}, {Self, OtherKey}}, {self(), Ref}})
                                    end,

                                    % reply先がNeighborの更新に成功するまで待機
                                    receive
                                        {ok, Ref} ->
                                            Update(),

                                            case ets:lookup('ETS-Table', Server) of
                                                [] ->
                                                    spawn(fun() ->
                                                                Tab = gen_server:call(Server, {'get-ets-table'}),
                                                                ets:insert('ETS-Table', {Server, Tab})
                                                        end);
                                                _ ->
                                                    ok
                                            end;

                                        {error, Ref, Message} ->
                                            pass
                                    end
                            end,

                            case OtherNode of
                                % 自分自身の場合，直接update関数を呼び出す
                                Self ->
                                    io:format("update1, SelfKey=~p, OtherKey=~p, NewKey=~p~n", [SelfKey, OtherKey, NewKey]),

                                    Update = fun() ->
                                            update(SelfKey, {Server, NewKey}, Level),
                                            update(OtherKey, {Server, NewKey}, Level)
                                    end,

                                    F(Update);

                                _ ->
                                    Update = fun() ->
                                            gen_server:call(OtherNode,
                                                {OtherKey,
                                                    {'join-process-2',
                                                        {From,
                                                            Server,
                                                            NewKey}},
                                                    Level,
                                                    {whereis(?SERVER_MODULE), SelfKey}}),
                                            update(SelfKey, {Server, NewKey}, Level)
                                    end,

                                    F(Update)
                            end
                    end
            end;

        % MembershipVector[Level]が一致しないので(外側に向かって)探索を続ける
        _ ->
            Peer = if
                NewKey < SelfKey ->
                    case Level of
                        0 ->
                            % Levelが0の時は必ずMembershipVectorが一致するはずなのでありえない
                            error;
                        _ ->
                            lists:nth(Level, Bigger)
                    end;

                SelfKey < NewKey ->
                    case Level of
                        0 ->
                            % Levelが0の時は必ずMembershipVectorが一致するはずなのでありえない
                            error;
                        _ ->
                            lists:nth(Level, Smaller)
                    end
            end,

            case Peer of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {error, mismatch});

                {NextNode, NextKey} ->
                    spawn(fun() ->
                                gen_server:call(NextNode,
                                    {NextKey,
                                        {'join-process-1',
                                            {From,
                                                Server,
                                                NewKey,
                                                MembershipVector}},
                                        Level})
                        end)
            end
    end.


%% process_0/3関数と同じだが，process_1_oneway/3関数を呼び出す

process_0_oneway(SelfKey, {From, _Server, NewKey, _MembershipVector}, Level)
when Level == 0 andalso SelfKey == NewKey ->
    gen_server:reply(From, {exist, {whereis(?SERVER_MODULE), SelfKey}});

process_0_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level)
when Level > 0 andalso SelfKey == NewKey ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?SERVER_MODULE,
                            {SelfKey,
                                {'join-process-0-oneway',
                                    {From,
                                        Server,
                                        NewKey,
                                        MembershipVector}},
                                Level})
                end);

        _ ->
            [{SelfKey, {_, _, {Smaller, _}}}] = ets:lookup('Peer', SelfKey),

            case lists:nth(Level, Smaller) of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {error, mismatch});

                {BestNode, BestKey} ->
                    spawn(fun() ->
                                gen_server:call(BestNode,
                                    {BestKey,
                                        {'join-process-0-oneway',
                                            {From,
                                                Server,
                                                NewKey,
                                                MembershipVector}},
                                        Level})
                        end)
            end
    end;

process_0_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?SERVER_MODULE,
                            {SelfKey,
                                {'join-process-0-oneway',
                                    {From,
                                        Server,
                                        NewKey,
                                        MembershipVector}},
                                Level})
                end);

        _ ->
            [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

            {Neighbor, S_or_B} = if
                NewKey < SelfKey ->
                    {Smaller, smaller};
                SelfKey < NewKey ->
                    {Bigger, bigger}
            end,

            case Level of
                0 ->
                    case select_best(Neighbor, NewKey, S_or_B) of
                        % 最適なピアが見つかったので、次のフェーズ(process_1_oneway)へ移行．
                        % 他の処理をlockしておくために関数として(process_1_onewayフェーズへ)移行する．
                        {'__none__', '__none__'} ->
                            process_1_oneway(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);
                        {'__self__'} ->
                            process_1_oneway(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);


                        % 最適なピアの探索を続ける(join-process-0-oneway)
                        {BestNode, BestKey} ->
                            spawn(fun() ->
                                        gen_server:call(BestNode,
                                            {BestKey,
                                                {'join-process-0-oneway',
                                                    {From,
                                                        Server,
                                                        NewKey,
                                                        MembershipVector}},
                                                Level})
                                end)
                    end;

                % LevelN(N > 0)の場合，Level(N - 1)以下にはNewKeyピアが存在するので，特別な処理が必要
                _ ->
                    Peer = case ets:lookup('Incomplete', SelfKey) of
                        [{SelfKey, {join, MaxLevel}}] when MaxLevel + 1 == Level ->
                            % MaxLevel + 1 == Levelより，lists:nth(MaxLevel + 1, Neighbor) == lists:nth(Level, Neigbor)
                            % なのであまり意味は無い
                            lists:nth(MaxLevel + 1, Neighbor);

                        [{SelfKey, {join, MaxLevel}}] when MaxLevel < Level ->
                            % バグが無ければ，このパターンになることはない
                            error;

                        _ ->
                            lists:nth(Level, Neighbor)
                    end,

                    case Peer of
                        % Neighbor[Level]がNewKey => Level上で，SelfKeyはNewKeyの隣のピア
                        {_, NewKey} ->
                            process_1_oneway(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);

                        _ ->
                            % Level(N - 1)以上のNeighborを対象にすることで，無駄なメッセージング処理を無くす
                            BestPeer = case ets:lookup('Incomplete', SelfKey) of
                                [{SelfKey, {join, MaxLevel_}}] when (MaxLevel_ + 1) == Level ->
                                    select_best(lists:nthtail(MaxLevel_, Neighbor), NewKey, S_or_B);
                                [{SelfKey, {join, MaxLevel_}}] when MaxLevel_ < Level ->
                                    % バグが無ければ，このパターンになることはない
                                    error;
                                _ ->
                                    select_best(lists:nthtail(Level - 1, Neighbor), NewKey, S_or_B)
                            end,

                            case BestPeer of
                                {'__none__', '__none__'} ->
                                    process_1_oneway(SelfKey,
                                        {From,
                                            Server,
                                            NewKey,
                                            MembershipVector},
                                        Level);
                                {'__self__'} ->
                                    process_1_oneway(SelfKey,
                                        {From,
                                            Server,
                                            NewKey,
                                            MembershipVector},
                                        Level);


                                % 最適なピアの探索を続ける(join-process-0-oneway)
                                {BestNode, BestKey} ->
                                    spawn(fun() ->
                                                gen_server:call(BestNode,
                                                    {BestKey,
                                                        {'join-process-0-oneway',
                                                            {From,
                                                                Server,
                                                                NewKey,
                                                                MembershipVector}},
                                                        Level})
                                        end)
                            end
                    end
            end
    end.


%% process_0_oneway/3関数から呼び出される．
%% 基本はprocess_1/3関数と同じだが，片方向にしか探索しない．
process_1_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    io:format("join:process_1_oneway: SelfKey=~p, NewKey=~p, Level=~p~n", [SelfKey, NewKey, Level]),
    [{SelfKey, {_, SelfMembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    TailN = ?LEVEL_MAX - Level - 1,
    <<_:TailN, Bit:1, _:Level>> = MembershipVector,
    <<_:TailN, SelfBit:1, _:Level>> = SelfMembershipVector,

    case Bit of
        SelfBit ->
            case ets:lookup('Incomplete', SelfKey) of
                [{SelfKey, {join, MaxLevel}}] when MaxLevel < Level ->
                    Neighbor = if
                        NewKey < SelfKey ->
                            Bigger;
                        SelfKey < NewKey ->
                            Smaller
                    end,

                    case lists:nth(MaxLevel + 1, Neighbor) of
                        {'__none__', '__none__'} ->
                            gen_server:reply(From, {error, mismatch});

                        %{OtherNode, OtherKey} ->
                        %    spawn(fun() ->
                        %            gen_server:call(OtherNode,
                        %                {OtherKey,
                        %                    {'join-process-1-oneway',
                        %                        {From,
                        %                            Server,
                        %                            NewKey,
                        %                            MembershipVector}},
                        %                    Level})
                        %    end)
                        _ ->
                            io:format("~n~n__incomplete_error__ SelfKey=~p, NewKey=~p, Level=~p~n~n", [SelfKey, NewKey, Level]),
                            [{{SelfKey, MaxLevel}, Daemon}] = ets:lookup('Joining-Wait', {SelfKey, MaxLevel}),
 
                            Ref = make_ref(),
                            Pid = spawn(fun() ->
                                        receive
                                            {ok, Ref} ->
                                                gen_server:call(?SERVER_MODULE,
                                                    {SelfKey,
                                                        {'join-process-0-oneway',
                                                            {From,
                                                                Server,
                                                                NewKey,
                                                                MembershipVector}},
                                                        Level});

                                            {error, Ref, Message} ->
                                                pass
                                        end
                                end),

                            Daemon ! {add, {Pid, Ref}}
                end;

                _ ->
                    Ref = make_ref(),

                    gen_server:reply(From,
                        {ok, {whereis(?SERVER_MODULE), SelfKey}, {self(), Ref}}),

                    % reply先がNeighborの更新に成功するまで待機
                    receive
                        {ok, Ref} ->
                            update(SelfKey, {Server, NewKey}, Level),
                            case ets:lookup('ETS-Table', Server) of
                                [] ->
                                    spawn(fun() ->
                                                Tab = gen_server:call(Server, {'get-ets-table'}),
                                                ets:insert('ETS-Table', {Server, Tab})
                                        end);
                                _ ->
                                    ok
                            end;

                        {error, Ref, Message} ->
                            pass
                    end

            end;

        _ ->
            Peer = if
                NewKey < SelfKey ->
                    case Level of
                        0 ->
                            lists:nth(Level + 1, Bigger);
                        _ ->
                            lists:nth(Level, Bigger)
                    end;

                SelfKey < NewKey ->
                    case Level of
                        0 ->
                            lists:nth(Level + 1, Smaller);
                        _ ->
                            lists:nth(Level, Smaller)
                    end
            end,

            case Peer of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {error, mismatch});

                {NextNode, NextKey} ->
                    spawn(fun() ->
                                gen_server:call(NextNode,
                                    {NextKey,
                                        {'join-process-1-oneway',
                                            {From,
                                                Server,
                                                NewKey,
                                                MembershipVector}},
                                        Level})
                        end)
            end
    end.

