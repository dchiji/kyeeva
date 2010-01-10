%%    Copyright 2009  CHIJIWA Daiki <daiki41@gmail.com>
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

%%    TODO
%%
%%      * {join, NewKey}で，適当なピアの選択時にNewKeyを有効活用する
%%      * join中にサーバが再起動した場合の処理
%%      * get時に死んだノードを発見した場合の処理

-module(skipgraph).
-behaviour(gen_server).

-export([start/1, init/1, handle_call/3, terminate/2,
        handle_cast/2, handle_info/2, code_change/3,
        join/1, join/2, remove/1, test/0, get/1, get/2, put/2]).

-export([get_server/0, get_peer/0]).

%-define(LEVEL_MAX, 8).
%-define(LEVEL_MAX, 16).
-define(LEVEL_MAX, 32).
%-define(LEVEL_MAX, 64).
%-define(LEVEL_MAX, 128).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).


%%--------------------------------------------------------------------
%% Function: start
%% Description(ja): 初期化を行い，gen_serverを起動する．
%% Description(en): 
%% Returns: Server
%%--------------------------------------------------------------------
start(Initial) ->
    case Initial of
        '__none__' ->
            %gen_server:start_link(
            %    {local, ?MODULE},
            %    ?MODULE,
            %    ['__none__',
            %        make_membership_vector()],
            %    [{debug, [trace, log]}]);
            gen_server:start_link(
                {local, ?MODULE},
                ?MODULE,
                ['__none__',
                    make_membership_vector()],
                []);

        _ ->
            InitialNode = rpc:call(Initial, skipgraph, get_server, []),
            %gen_server:start_link(
            %    {local, ?MODULE},
            %    ?MODULE,
            %    [InitialNode,
            %        make_membership_vector()],
            %    [{debug, [trace, log]}])
            gen_server:start_link(
                {local, ?MODULE},
                ?MODULE,
                [InitialNode,
                    make_membership_vector()],
                [])
    end.



%%====================================================================
%% gen_server callbacks
%%====================================================================

%% gen_server:start_linkにより呼び出される．
init(Arg) ->
    % {Key, {Value, MembershipVector, Neighbor}}
    %   MembershipVector : <<1, 0, 1, 1, ...>>
    %   Neighbor : {Smaller, Bigger}
    %     Smaller : [{Node, Key}, ...]
    %     Smaller : [{Node, Key}, ...]
    T = ets:new('Peer', [ordered_set, public, named_table]),

    ets:new('Lock-Update-Daemon', [set, public, named_table]),
    ets:new('Lock-Join-Daemon', [set, public, named_table]),
    ets:new('Joining-Wait', [set, public, named_table]),
    ets:new('Incomplete', [set, public, named_table]),
    ets:new('ETS-Table', [set, public, named_table]),

    ets:insert('ETS-Table', {?MODULE, T}),

    {ok, Arg}.


%%--------------------------------------------------------------------
%% Function: handle_call <peer>
%% Description(ja): ネットワーク先のノードから呼び出される．適当なピアを
%%                  呼び出し元に返す．
%% Description(en): 
%% Returns: {Pid, Key}
%%--------------------------------------------------------------------
handle_call({peer, random}, _From, State) ->
    [{SelfKey, {_, _, _}} | _] = ets:tab2list('Peer'),
    {reply, {whereis(?MODULE), SelfKey}, State};


%%--------------------------------------------------------------------
%% Function: handle_call <get-ets-table>
%% Description(ja): ネットワーク先のノードから呼び出される．自ノードの
%%                  Peerを保持しているETSテーブルを呼び出し元に返す．
%%                  これによりget時の速度を向上させることができる．
%% Description(en): 
%% Returns: ETSTable
%%--------------------------------------------------------------------
handle_call({'get-ets-table'}, _From, State) ->
    [{?MODULE, Tab}] = ets:lookup('ETS-Table', ?MODULE),
    {reply, Tab, State};


%% ピア(SelfKey)のValueを書き換える．join処理と組み合わせて使用．
handle_call({SelfKey, {put, Value}}, _From, State) ->
    F = fun([{_SelfKey, {_Value, MembershipVector, Neighbor}}]) ->
            {ok, {SelfKey, {Value, MembershipVector, Neighbor}}}
    end,

    lock_update(SelfKey, F),
    {reply, ok, State};


%% getメッセージを受信し、適当なローカルピア(無ければグローバルピア)を選択して，get_process_0に繋げる．
handle_call({get, Key0, Key1},
    From,
    [InitialNode | Tail]) ->

    F = fun() ->
            PeerList = ets:tab2list('Peer'),
            case PeerList of
                [] ->
                    case InitialNode of
                        '__none__' ->
                            gen_server:reply(From,
                                {ok, {'__none__', '__none__'}});

                        _ ->
                            {_, InitKey} = gen_server:call(InitialNode, {peer, random}),
                            gen_server:call(InitialNode,
                                {InitKey,
                                    {'get-process-0',
                                        {Key0, Key1, From}}})
                    end;

                _ ->
                    [{SelfKey, {_, _, _}} | _] = PeerList,
                    gen_server:call(?MODULE,
                        {SelfKey,
                            {'get-process-0',
                                {Key0, Key1, From}}})
            end
    end,

    spawn(F),
    {noreply, [InitialNode | Tail]};


%% 最適なピアを探索する．
handle_call({SelfKey, {'get-process-0', {Key0, Key1, From}}},
    _From,
    State) ->

    F = fun() ->
            [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

            {Neighbor, S_or_B} = if
                Key0 =< SelfKey -> {Smaller, smaller};
                SelfKey < Key0 -> {Bigger, bigger}
            end,

            case select_best(Neighbor, Key0, S_or_B) of
                % 最適なピアが見つかったので、次のフェーズ(get-process-1)へ移行
                {'__none__', '__none__'} ->
                    gen_server:call(?MODULE,
                        {SelfKey,
                            {'get-process-1',
                                {Key0, Key1, From},
                                []}});
                {'__self__'} ->
                    gen_server:call(?MODULE,
                        {SelfKey,
                            {'get-process-1',
                                {Key0, Key1, From},
                                []}});

                % 探索するキーが一つで，かつそれを保持するピアが存在した場合，ETSテーブルに直接アクセスする
                {BestNode, Key0} when Key0 == Key1 ->
                    case ets:lookup('ETS-Table', BestNode) of
                        [{BestNode, Tab}] ->
                            [{BestNode, Tab} | _] = ets:lookup('ETS-Table', BestNode),
                            case ets:lookup(Tab, Key0) of
                                [{Key0, {Value, _, _}} | _] ->
                                    io:format("ets~n"),
                                    gen_server:reply(From, {ok, [{Key0, Value}]});

                                [] ->
                                    gen_server:reply(From, {ok, []})
                            end;

                        _ ->
                            spawn(fun() ->
                                        gen_server:call(BestNode,
                                            {Key0,
                                                {'get-process-0',
                                                    {Key0, Key1, From}}})
                                end)
                    end;

                % 最適なピアの探索を続ける(get-process-0)
                {BestNode, BestKey} ->
                    spawn(fun() ->
                                gen_server:call(BestNode,
                                    {BestKey,
                                        {'get-process-0',
                                            {Key0, Key1, From}}})
                        end)
            end
    end,

    spawn(F),
    {reply, '__none__', State};


%% 指定された範囲内を走査し，値を収集する．
handle_call({SelfKey, {'get-process-1', {Key0, Key1, From}, ItemList}},
    _From,
    State) ->

    F = fun() ->
            if
                SelfKey < Key0 ->
                    [{SelfKey, {_, _, {_, [{NextNode, NextKey} | _]}}}] = ets:lookup('Peer', SelfKey),
                    case {NextNode, NextKey} of
                        {'__none__', '__none__'} ->
                            gen_server:reply(From, {ok, ItemList});
                        _ ->
                            gen_server:call(NextNode,
                                {NextKey,
                                    {'get-process-1',
                                        {Key0, Key1, From},
                                        ItemList}})
                    end;

                Key1 < SelfKey ->
                    gen_server:reply(From, {ok, ItemList});

                true ->
                    [{SelfKey, {Value, _, {_, [{NextNode, NextKey} | _]}}}] = ets:lookup('Peer', SelfKey),
                    case {NextNode, NextKey} of
                        {'__none__', '__none__'} ->
                            gen_server:reply(From, {ok, [{SelfKey, Value} | ItemList]});
                        _ ->
                            gen_server:call(NextNode,
                                {NextKey,
                                    {'get-process-1',
                                        {Key0, Key1, From},
                                        [{SelfKey, Value} | ItemList]}})
                    end
            end
    end,

    spawn(F),
    {noreply, State};


%% joinメッセージを受信し、適当なローカルピア(無ければグローバルピア)を返す．
handle_call({join, _NewKey},
    From,
    [InitialNode | Tail]) ->

    PeerList = ets:tab2list('Peer'),

    case PeerList of
        [] ->
            case InitialNode of
                '__none__' ->
                    gen_server:reply(From,
                        {ok, {'__none__', '__none__'}});
                _ ->
                    {_, Key} = gen_server:call(InitialNode, {peer, random}),
                    gen_server:reply(From, {ok, {InitialNode, Key}})
            end;

        _ ->
            [{SelfKey, {_, _, _}} | _] = PeerList,
            gen_server:reply(From, {ok, {whereis(?MODULE), SelfKey}})
    end,

    {noreply, [InitialNode | Tail]};


%% join-process-0にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'join-process-0', {Server, NewKey, MembershipVector}}, Level},
    From,
    State) ->

    spawn(fun() ->
                gen_server:call(whereis(?MODULE),
                    {SelfKey,
                        {'join-process-0',
                            {From,
                                Server,
                                NewKey,
                                MembershipVector}},
                        Level})
        end),

    {noreply, State};

%% join_process_0/3関数をlock_joinに与える
handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join_process_0(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    lock_join(SelfKey, F),
    {noreply, State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% join_process_1を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'join-process-1', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join_process_1(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    lock_join(SelfKey, F),
    {reply, '__none__', State};


%% join-process-0-onewayにFromを渡して再度呼び出す．
handle_call({SelfKey, {'join-process-0-oneway', {Server, NewKey, MembershipVector}}, Level},
    From,
    State) ->

    spawn(fun() ->
                gen_server:call(whereis(?MODULE),
                    {SelfKey,
                        {'join-process-0-oneway',
                            {From,
                                Server,
                                NewKey,
                                MembershipVector}},
                        Level})
        end),

    {noreply, State};

%% join_process_0_oneway/3関数をlock_joinに与える
handle_call({SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join_process_0_oneway(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    lock_join(SelfKey, F),
    {noreply, State};


%% join_process_1_oneway/3関数を呼び出す．ローカルからこのコールバック関数を呼び出すことはほとんどない．
handle_call({SelfKey, {'join-process-1-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join_process_1_oneway(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    lock_join(SelfKey, F),
    {reply, '__none__', State};


%% ローカルで呼び出すことはない．Neighborをupdateする．
handle_call({SelfKey, {'join-process-2', {From, Server, NewKey}}, Level, Other},
    _From,
    State) ->

    F = fun() ->
            Ref = make_ref(),
            update(SelfKey, {Server, NewKey}, Level),

            if
                NewKey < SelfKey ->
                    gen_server:reply(From,
                        {ok,
                            {Other,
                                {whereis(?MODULE), SelfKey}},
                            {self(), Ref}});

                SelfKey < NewKey ->
                    gen_server:reply(From,
                        {ok,
                            {{whereis(?MODULE), SelfKey},
                                Other},
                            {self(), Ref}})
            end,

            % reply先がNeighborの更新に成功するまで待機
            receive
                {ok, Ref} ->
                    ok
            end,

            case ets:lookup('ETS-Table', Server) of
                [] ->
                    spawn(fun() ->
                                Tab = gen_server:call(Server, {'get-ets-table'}),
                                ets:insert('ETS-Table', {Server, Tab})
                        end);
                _ ->
                    ok
            end
    end,

    lock_join(SelfKey, F),
    {reply, '__none__', State};


%% remove-process-0にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'remove-process-0', {RemovedKey}}, Level},
    From,
    State) ->

    spawn(fun() ->
                gen_server:call(whereis(?MODULE),
                    {SelfKey,
                        {'remove-process-0',
                            {From,
                                RemovedKey}},
                        Level})
        end),

    {noreply, State};

%% remove_process_0/3関数をlock_joinに与える
handle_call({SelfKey, {'remove-process-0', {From, RemovedKey}}, Level},
    _From,
    State) ->

    F = fun() ->
            remove_process_0(SelfKey,
                {From,
                    RemovedKey},
                Level)
    end,

    lock_join(SelfKey, F),
    {reply, '__none__', State};


%% remove-process-1にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'remove-process-1', {RemovedKey}}, Level},
    From,
    State) ->

    spawn(fun() ->
                gen_server:call(whereis(?MODULE),
                    {SelfKey,
                        {'remove-process-1',
                            {From,
                                RemovedKey}},
                        Level})
        end),

    {noreply, State};

%% ネットワーク先ノードから呼び出されるために存在する．
%% remove_process_1を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-1', {From, RemovedKey}}, Level},
    _From,
    State) ->

    F = fun() ->
            remove_process_1(SelfKey,
                {From,
                    RemovedKey},
                Level)
    end,

    lock_join(SelfKey, F),
    {reply, '__none__', State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% remove_process_2を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-2', {From, RemovedKey}}, NewNeighbor, Level},
    _From,
    State) ->

    F = fun() ->
            remove_process_2(SelfKey,
                {From,
                    RemovedKey},
                NewNeighbor,
                Level)
    end,

    lock_join(SelfKey, F),
    {reply, '__none__', State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% remove_process_3を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-3', {From, RemovedKey}}, NewNeighbor, Level},
    _From,
    State) ->

    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {OldNode, OldKey} = if
        SelfKey < RemovedKey ->
            lists:nth(Level + 1, Smaller);
        RemovedKey < SelfKey ->
            lists:nth(Level + 1, Bigger)
    end,

    case OldKey of
        RemovedKey ->
            update(SelfKey, NewNeighbor, Level);
        _ ->
            pass
    end,

    {reply, '__none__', State}.


%% gen_server内でエラーが発生したとき，再起動させる．
terminate(_Reason, State) ->
    spawn(fun() ->
                test(),
                unregister(?MODULE),
                gen_server:start_link({local, ?MODULE}, ?MODULE, State, [{debug, [trace, log]}])
        end),
    ok.


handle_cast(_Message, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _NewVsn) ->
    {ok, State}.



%%====================================================================
%% join-process callbacks
%%====================================================================

%% handle_call<join-process-0>から呼び出される．
%% 最もNewKeyに近いピアを(内側に向かって)探索する．
join_process_0(SelfKey, {From, _Server, NewKey, _MembershipVector}, Level)
when Level == 0 andalso SelfKey == NewKey ->
    gen_server:reply(From, {exist, {whereis(?MODULE), SelfKey}});

join_process_0(SelfKey, {From, Server, NewKey, MembershipVector}, Level)
when Level > 0 andalso SelfKey == NewKey ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?MODULE,
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

join_process_0(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?MODULE,
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
                        % 最適なピアが見つかったので、次のフェーズ(join_process_1)へ移行．
                        % 他の処理をlockしておくために関数として(join_process_1フェーズへ)移行する．
                        {'__none__', '__none__'} ->
                            join_process_1(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);
                        {'__self__'} ->
                            join_process_1(SelfKey,
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
                            join_process_1(SelfKey,
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
                                    join_process_1(SelfKey,
                                        {From,
                                            Server,
                                            NewKey,
                                            MembershipVector},
                                        Level);
                                {'__self__'} ->
                                    join_process_1(SelfKey,
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


%% join_process_0/3関数から呼び出される．
%% MembershipVector[Level]が一致するピアを外側に向かって探索．
%% 成功したら自身のNeighborをupdateし，Anotherにもupdateメッセージを送信する．
join_process_1(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    io:format("join_process_1: SelfKey=~p, NewKey=~p, Level=~p~n", [SelfKey, NewKey, Level]),
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
                                                gen_server:call(?MODULE,
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
                            {Smaller, {ok, {{'__none__', '__none__'}, {whereis(?MODULE), SelfKey}}, {self(), Ref}}};
                        SelfKey < NewKey ->
                            {Bigger, {ok, {{whereis(?MODULE), SelfKey}, {'__none__', '__none__'}}, {self(), Ref}}}
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
                            Self = whereis(?MODULE),

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
                                                    {whereis(?MODULE), SelfKey}}),
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


%% join_process_0/3関数と同じだが，join_process_1_oneway/3関数を呼び出す

join_process_0_oneway(SelfKey, {From, _Server, NewKey, _MembershipVector}, Level)
when Level == 0 andalso SelfKey == NewKey ->
    gen_server:reply(From, {exist, {whereis(?MODULE), SelfKey}});

join_process_0_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level)
when Level > 0 andalso SelfKey == NewKey ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?MODULE,
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

join_process_0_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, {join, -1}}] ->
            io:format("~n~nrecall SelfKey=~p, NewKey=~p~n~n", [SelfKey, NewKey]),

            spawn(fun() ->
                        timer:sleep(2),
                        gen_server:call(?MODULE,
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
                        % 最適なピアが見つかったので、次のフェーズ(join_process_1_oneway)へ移行．
                        % 他の処理をlockしておくために関数として(join_process_1_onewayフェーズへ)移行する．
                        {'__none__', '__none__'} ->
                            join_process_1_oneway(SelfKey,
                                {From,
                                    Server,
                                    NewKey,
                                    MembershipVector},
                                Level);
                        {'__self__'} ->
                            join_process_1_oneway(SelfKey,
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
                            join_process_1_oneway(SelfKey,
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
                                    join_process_1_oneway(SelfKey,
                                        {From,
                                            Server,
                                            NewKey,
                                            MembershipVector},
                                        Level);
                                {'__self__'} ->
                                    join_process_1_oneway(SelfKey,
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


%% join_process_0_oneway/3関数から呼び出される．
%% 基本はjoin_process_1/3関数と同じだが，片方向にしか探索しない．
join_process_1_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->
    io:format("join_process_1_oneway: SelfKey=~p, NewKey=~p, Level=~p~n", [SelfKey, NewKey, Level]),
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
                                                gen_server:call(?MODULE,
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
                        {ok, {whereis(?MODULE), SelfKey}, {self(), Ref}}),

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



%%====================================================================
%% remove-process callbacks
%%====================================================================

%% handle_call<remove-process-0>から呼び出される．
%% RemovedKeyピアを探索．
%% ただ，ほとんど使用しない．
remove_process_0(SelfKey, {From, RemovedKey}, Level) ->
    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {IsSelf, {NeighborNode, NeighborKey}, S_or_B} = if
        SelfKey == RemovedKey ->
            {true, lists:nth(Level + 1, Smaller), smaller};
        SelfKey < RemovedKey ->
            {false, lists:nth(Level + 1, Bigger), bigger};
        RemovedKey < SelfKey ->
            {false, lists:nth(Level + 1, Smaller), smaller}
    end,

    case IsSelf of
        true ->
            spawn(fun() ->
                        remove(RemovedKey)
                end);

        false ->
            spawn(fun() ->
                        gen_server:call(NeighborNode,
                            {NeighborKey,
                                {'remove-process-0',
                                    {From,
                                        RemovedKey},
                                    Level}})
                end)
    end.


%% handle_call<remove-process-1>から呼び出される．
%% このとき，SelfKey == RemovedKeyはtrue．
%% 片方のNeighborにremove-process-2メッセージを送信．
remove_process_1(SelfKey, {From, RemovedKey}, Level) ->
    io:format("Peer[SelfKey] = ~p~n", [ets:lookup('Peer', SelfKey)]),

    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    case lists:nth(Level + 1, Smaller) of
        {'__none__', '__none__'} ->
            case lists:nth(Level + 1, Bigger) of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {ok, removed});

                {BiggerNode, BiggerKey} ->
                    spawn(fun() ->
                                gen_server:call(BiggerNode,
                                    {BiggerKey,
                                        {'remove-process-2',
                                            {From,
                                                RemovedKey}},
                                        Smaller,
                                        Level},
                                    ?TIMEOUT)
                        end)
            end;

        {SmallerNode, SmallerKey} ->
            spawn(fun() ->
                        gen_server:call(SmallerNode,
                            {SmallerKey,
                                {'remove-process-2',
                                    {From,
                                        RemovedKey}},
                                Bigger,
                                Level},
                            ?TIMEOUT)
                end)
    end.


%% handle_call<remove-process-2>から呼び出される．
%% 実際にremove処理を行い，NewNeighborにremove-process-3メッセージを送信．
remove_process_2(SelfKey, {From, RemovedKey}, NewNeighbor, Level) ->
    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {OldNode, OldKey} = if
        SelfKey < RemovedKey ->
            lists:nth(Level + 1, Smaller);
        RemovedKey < SelfKey ->
            lists:nth(Level + 1, Bigger)
    end,

    case OldKey of
        RemovedKey ->
            case NewNeighbor of
                {'__none__', '__none__'} ->
                    update(SelfKey, NewNeighbor, Level);

                {NewNode, NewKey} ->
                    gen_server:call(NewNode,
                        {NewKey,
                            {'remove-process-3',
                                {From,
                                    RemovedKey},
                                {whereis(?MODULE), SelfKey},
                                Level}}),

                    update(SelfKey, NewNeighbor, Level)
            end;

        _ ->
            pass
    end,

    gen_server:reply(From, {ok, removed}).



%%====================================================================
%% Utilities (Transactions)
%%====================================================================

%% {trap}を受信したら，PListに含まれる全プロセスに{ok, Ref}メッセージを送信する．
wait_trap(PList) ->
    receive
        {add, {Pid, Ref}} ->
            wait_trap([{Pid, Ref} | PList]);

        {trap} ->
            io:format("trap~n"),
            lists:foreach(fun({Pid, Ref}) ->
                        Pid ! {ok, Ref}
                end,
                PList),

            wait_trapped({trap});

        {error, Message} ->
            Message = 
            lists:foreach(fun({Pid, Ref}) ->
                        Pid ! {error, Ref, Message}
                end,
                PList),

            wait_trapped({error, Message})
    end.

%% wait_trap/1関数が{trap}を受信した後の処理．
wait_trapped(Receive) ->
    receive
        {add, {Pid, Ref}} ->
            case Receive of
                {trap} ->
                    Pid ! {ok, Ref};
                {error, Message} ->
                    Pid ! {error, Ref, Message}
            end
    end,

    wait_trapped(Receive).



%% キー毎に独立したプロセスとして常駐する
lock_daemon(F) ->
    receive
        Message ->
            F(Message)
    end,

    lock_daemon(F).


%% 任意のキーに対してlock_daemonプロセスを生成し，ロックされた安全なjoinを行う
lock_join(Key, F) ->
    Ref = make_ref(),

    case ets:lookup('Lock-Join-Daemon', Key) of
        [] ->
            ets:insert('Lock-Join-Daemon',
                {Key,
                    spawn(fun() ->
                                lock_daemon(fun lock_join_callback/1)
                        end)}),

            [{Key, Daemon}] = ets:lookup('Lock-Join-Daemon', Key),
            Daemon ! {{self(), Ref}, {update, F}};

        [{Key, Daemon}] ->
            Daemon ! {{self(), Ref}, {update, F}}
    end,

    receive
        {Ref, ok} -> ok
    end.

%% lock_daemon用のコールバック関数．安全なjoinを行う
lock_join_callback({{From, Ref}, {update, F}}) ->
    F(),
    From ! {Ref, ok}.


%% 任意のキーに対してlock_daemonプロセスを生成し，ロックされた安全なupdateを行う

lock_update(Key, F) ->
    lock_update('Peer', Key, F).

lock_update(Table, Key, F) ->
    Ref = make_ref(),

    case ets:lookup('Lock-Update-Daemon', Key) of
        [] ->
            ets:insert('Lock-Update-Daemon',
                {Key,
                    spawn(fun() ->
                                lock_daemon(fun lock_update_callback/1)
                        end)}),

            [{{Table, Key}, Daemon}] = ets:lookup('Lock-Update-Daemon', Key),
            Daemon ! {{self(), Ref}, {update, Table, {Key, F}}};

        [{Key, Daemon}] ->
            Daemon ! {{self(), Ref}, {update, Table, {Key, F}}}
    end,

    receive
        {Ref, Result} -> Result
    end.

%% lock_daemon用のコールバック関数．
%% lock_update関数によって関連づけられる．
%% ETSテーブルを安全に更新する
lock_update_callback({{From, Ref}, {update, Table, {Key, F}}}) ->
    Item = ets:lookup(Table, Key),

    case F(Item) of
        {ok, Result} ->
            ets:insert(Table, Result),
            From ! {Ref, {ok, Result}};

        {delete, DeletedKey} ->
            ets:delete(Table, DeletedKey),
            From ! {Ref, {delete, DeletedKey}};

        {error, _Reason} ->
            From ! {Ref, {pass}};

        {pass} ->
            From ! {Ref, {ok, []}}
    end.


%% lock_updateを利用してNeighbor[Level]を更新する
update(SelfKey, {Server, NewKey}, Level) ->
    F = fun([{_, {Value, MembershipVector, {Smaller, Bigger}}}]) ->
            io:format("NewKey=~p  SelfKey=~p~n", [NewKey, SelfKey]),
            if
                {Server, NewKey} == {'__none__', '__none__'} ->
                    {SFront, [_ | STail]} = lists:split(Level, Smaller),
                    {BFront, [_ | BTail]} = lists:split(Level, Bigger),

                    NewSmaller = SFront ++ [{Server, NewKey} | STail],
                    NewBigger = BFront ++ [{Server, NewKey} | BTail],

                    io:format("update: SelfKey=~p, NewKey=~p, Level=~p  ~p~n", [SelfKey, NewKey, Level, smaller]),
                    {ok, {SelfKey, {Value, MembershipVector, {NewSmaller, NewBigger}}}};

                NewKey < SelfKey ->
                    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Smaller),

                    NewSmaller = Front ++ [{Server, NewKey} | Tail],
                    io:format("update: SelfKey=~p, NewKey=~p, Level=~p  ~p~n", [SelfKey, NewKey, Level, smaller]),
                    {ok, {SelfKey, {Value, MembershipVector, {NewSmaller, Bigger}}}};

                SelfKey < NewKey ->
                    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Bigger),

                    NewBigger = Front ++ [{Server, NewKey} | Tail],
                    io:format("update: SelfKey=~p, NewKey=~p, Level=~p  ~p~n", [SelfKey, NewKey, Level, bigger]),
                    {ok, {SelfKey, {Value, MembershipVector, {Smaller, NewBigger}}}}
            end
    end,

    lock_update(SelfKey, F).



%%====================================================================
%% Utilities (Other)
%%====================================================================

%% Neighborの中から最適なピアを選択する

select_best([], _, _) ->
    {'__none__', '__none__'};
select_best([{'__none__', '__none__'} | _], _, _) ->
    {'__none__', '__none__'};
select_best([{Node0, Key0} | _], Key, _)
when Key0 == Key ->
    {Node0, Key0};
select_best([{Node0, Key0}], Key, S_or_B)
when S_or_B == smaller ->
    if
        Key0 < Key ->
            {'__self__'};
        true ->
            {Node0, Key0}
    end;
select_best([{Node0, Key0}], Key, S_or_B)
when S_or_B == bigger ->
    if
        Key < Key0 ->
            {'__self__'};
        true ->
            {Node0, Key0}
    end;
select_best([{Node0, Key0}, {'__none__', '__none__'} | _], Key, S_or_B)
when S_or_B == smaller ->
    if
        Key0 < Key ->
            {'__self__'};
        true ->
            {Node0, Key0}
    end;
select_best([{Node0, Key0}, {'__none__', '__none__'} | _], Key, S_or_B)
when S_or_B == bigger ->
    if
        Key < Key0 ->
            {'__self__'};
        true ->
            {Node0, Key0}
    end;
select_best([{Node0, Key0}, {Node1, Key1} | Tail], Key, S_or_B)
when {Node0, Key0} == {Node1, Key1} ->    % rotate
    select_best([{Node1, Key1} | Tail], Key, S_or_B);
select_best([{Node0, Key0}, {Node1, Key1} | Tail], Key, S_or_B)
when S_or_B == smaller ->
    if
        Key0 < Key ->
            {'__self__'};
        Key1 < Key ->
            {Node0, Key0};
        Key < Key0 ->
            select_best([{Node1, Key1} | Tail], Key, S_or_B)
    end;
select_best([{Node0, Key0}, {Node1, Key1} | Tail], Key, S_or_B)
when S_or_B == bigger ->
    if
        Key < Key0 ->
            {'__self__'};
        Key < Key1 ->
            {Node0, Key0};
        true ->
            select_best([{Node1, Key1} | Tail], Key, S_or_B)
    end.


%% MembershipVectorを生成する

make_membership_vector() ->
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),

    N = random:uniform(256) - 1,
    case N rem 2 of
        1 ->
            make_membership_vector(<<N>>, ?LEVEL_MAX / 8 - 1);
        0 ->
            M = N - 1,
            make_membership_vector(<<M>>, ?LEVEL_MAX / 8 - 1)
    end.

make_membership_vector(Bin, 0.0) ->
    Bin;
make_membership_vector(Bin, N) ->
    make_membership_vector(<<(random:uniform(256) - 1):8, Bin/binary>>, N - 1).



%%====================================================================
%% Interfaces
%%====================================================================

%% 新しいピアをjoinする

join(Key) ->
    join(Key, '__none__').

join(Key, Value) ->
    case ets:lookup('Peer', Key) of
        [{Key, {_, _, _}}] ->
            F = fun([{SelfKey, {_Value, MembershipVector, Neighbor}}]) ->
                    {ok, {SelfKey, {Value, MembershipVector, Neighbor}}}
            end,

            lock_update(Key, F);

        [] ->
            MembershipVector = make_membership_vector(),
            Neighbor = {lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'}),
                lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'})},

            case gen_server:call(whereis(?MODULE), {join, Key}) of
                {ok, {'__none__', '__none__'}} ->
                    ets:insert('Peer', {Key, {Value, MembershipVector, Neighbor}}),
                    ets:insert('Lock-Join-Daemon',
                        {Key,
                            spawn(fun() ->
                                        lock_daemon(fun lock_join_callback/1)
                                end)}),
                    ets:insert('Lock-Update-Daemon',
                        {Key,
                            spawn(fun() ->
                                        lock_daemon(fun lock_update_callback/1)
                                end)});

                {ok, InitPeer} ->
                    ets:insert('Incomplete', {Key, {{join, -1}, {remove, ?LEVEL_MAX}}}),
                    ets:insert('Peer', {Key, {Value, MembershipVector, Neighbor}}),
                    ets:insert('Lock-Join-Daemon',
                        {Key,
                            spawn(fun() ->
                                        lock_daemon(fun lock_join_callback/1)
                                end)}),
                    ets:insert('Lock-Update-Daemon',
                        {Key,
                            spawn(fun() ->
                                        lock_daemon(fun lock_update_callback/1)
                                end)}),

                    join(InitPeer, Key, Value, MembershipVector)
            end
    end,
    ok.


join(InitPeer, Key, Value, MembershipVector) ->
    join(InitPeer, Key, Value, MembershipVector, 0, {'__none__', '__none__'}, 0).

join(_, Key, _, _, _, _, 100) ->
    F = fun(Item) ->
            case Item of
                [] ->
                    {pass};
                [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                    {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                [{Key, _}] ->
                    {delete, Key}
            end
    end,
    lock_update('Incomplete', Key, F);

join(_, Key, _, _, ?LEVEL_MAX, _, _) ->
    F = fun(Item) ->
            case Item of
                [] ->
                    {pass};
                [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                    {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                [{Key, _}] ->
                    {delete, Key}
            end
    end,
    lock_update('Incomplete', Key, F);

join({InitNode, InitKey}, NewKey, Value, MembershipVector, Level, OtherPeer, RetryN) ->
    Daemon = spawn(fun() -> wait_trap([]) end),
    ets:insert('Joining-Wait',
        {{NewKey, Level}, Daemon}),

    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-0',
                {whereis(?MODULE),
                    NewKey,
                    MembershipVector}},
            %Level}),
            Level},
        ?TIMEOUT),

    case Result of
        % 既に存在していた場合，そのピアにValueが上書きされる
        {exist, {Node, Key}} ->
            Self = whereis(?MODULE),

            case Node of
                Self ->
                    case ets:lookup('Incomplete', Key) of
                        [{Key, -1}] ->
                            ets:delete('Incomplete', Key),
                            gen_server:call(Node, {Key, {put, Value}});
                        [{Key, _}] ->
                            gen_server:call(Node, {Key, {put, Value}})
                    end;
                _ ->
                    ets:delete('Incomplete', Key),
                    ets:delete('Lock-Join-Daemon', Key),
                    ets:delete('Lock-Update-Daemon', Key),
                    ets:delete('Peer', Key),
                    gen_server:call(Node, {Key, {put, Value}})
            end,
            ok;

        {ok, {{'__none__', '__none__'}, {'__none__', '__none__'}}, _} ->
            F = fun(Item) ->
                    case Item of
                        [] ->
                            {pass};
                        [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                        [{Key, _}] ->
                            {delete, Key}
                    end
            end,
            lock_update('Incomplete', NewKey, F);

        {ok, {{'__none__', '__none__'}, BiggerPeer}, {Pid, Ref}} ->
            io:format("~nBiggerPeer=~p~n", [BiggerPeer]),

            update(NewKey, BiggerPeer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join_oneway(BiggerPeer, NewKey, Value, MembershipVector, Level + 1, 0);

        {ok, {SmallerPeer, {'__none__', '__none__'}}, {Pid, Ref}} ->
            io:format("~nSmallerPeer=~p~n", [SmallerPeer]),

            update(NewKey, SmallerPeer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join_oneway(SmallerPeer, NewKey, Value, MembershipVector, Level + 1, 0);

        {ok, {SmallerPeer, BiggerPeer}, {Pid, Ref}} ->
            io:format("~nSmallerPeer=~p, BiggerPeer=~p, {~p, ~p}~n", [SmallerPeer, BiggerPeer, Pid, Ref]),

            update(NewKey, SmallerPeer, Level),
            update(NewKey, BiggerPeer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join(SmallerPeer, NewKey, Value, MembershipVector, Level + 1, BiggerPeer, 0);

        {error, retry} ->
            timer:sleep(3000),
            join({InitNode, InitKey}, NewKey, Value, MembershipVector, Level, OtherPeer, RetryN + 1);

        {error, mismatch} ->
            case OtherPeer of
                {'__none__', '__none__'} ->
                    F = fun(Item) ->
                            case Item of
                                [] ->
                                    {pass};
                                [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                                    {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                                [{Key, _}] ->
                                    {delete, Key}
                            end
                    end,
                    lock_update('Incomplete', NewKey, F);

                _ ->
                    join_oneway(OtherPeer, NewKey, Value, MembershipVector, Level, 0)
            end;

        Message ->
            io:format("*ERR* join/5:unknown message: ~p~n", [Message]),
            error
    end.


%% 一方のNeighborが'__none__'のとき，もう一方のNeighborを通して新たなピアをjoinする

join_oneway(_, Key, _, _, _, 10) ->
    F = fun(Item) ->
            case Item of
                [] ->
                    {pass};
                [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                    {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                [{Key, _}] ->
                    {delete, Key}
            end
    end,
    lock_update('Incomplete', Key, F);

join_oneway(_, Key, _, _, ?LEVEL_MAX, _) ->
    F = fun(Item) ->
            case Item of
                [] ->
                    {pass};
                [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                    {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                [{Key, _}] ->
                    {delete, Key}
            end
    end,
    lock_update('Incomplete', Key, F);

join_oneway({InitNode, InitKey}, NewKey, Value, MembershipVector, Level, RetryN) ->
    Daemon = spawn(fun() -> wait_trap([]) end),
    ets:insert('Joining-Wait',
        {{NewKey, Level}, Daemon}),

    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-0-oneway',
                {whereis(?MODULE),
                    NewKey,
                    MembershipVector}},
            Level},
        ?TIMEOUT),

    %io:format("NewKey=~p, Level=~p, Result=~p~n", [NewKey, Level, Result]),

    case Result of
        {exist, {Node, Key}} ->
            Self = whereis(?MODULE),

            case Node of
                Self ->
                    case ets:lookup('Incomplete', Key) of
                        [{Key, {join, -1}}] ->
                            ets:delete('Incomplete', Key),
                            gen_server:call(Node, {Key, {put, Value}});
                        [{Key, {join, _}}] ->
                            gen_server:call(Node, {Key, {put, Value}})
                    end;
                _ ->
                    ets:delete('Peer', Key),
                    ets:delete('Incomplete', Key),
                    gen_server:call(Node, {Key, {put, Value}})
            end,
            ok;

        {ok, {'__none__', '__none__'}, _} ->
            F = fun(Item) ->
                    case Item of
                        [] ->
                            {pass};
                        [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                        [{Key, _}] ->
                            {delete, Key}
                    end
            end,
            lock_update('Incomplete', NewKey, F);

        {ok, Peer, {Pid, Ref}} ->
            update(NewKey, Peer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join_oneway(Peer, NewKey, Value, MembershipVector, Level + 1, 0);

        {error, retry} ->
            timer:sleep(3000),
            join_oneway({InitNode, InitKey}, NewKey, Value, MembershipVector, Level, RetryN + 1);

        {error, mismatch} ->
            F = fun(Item) ->
                    case Item of
                        [] ->
                            {pass};
                        [{Key, {{join, JLevel}, {remove, RLevel}}}] when RLevel /= ?LEVEL_MAX ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}};
                        [{Key, _}] ->
                            {delete, Key}
                    end
            end,
            lock_update('Incomplete', NewKey, F);

        Message ->
            io:format("*ERR* join/4:unknown message: ~p~n", [Message]),
            error
    end.


remove(Key) ->
    case ets:lookup('Peer', Key) of
        [] ->
            {error, noexist};

        [{Key, _}] ->
            F = fun(Item) ->
                    case Item of
                        [] ->
                            {ok, {Key, {{join, ?LEVEL_MAX}, {remove, ?LEVEL_MAX}}}};

                        [{Key, {{join, Max}, {remove, _}}}] ->
                            case Max of
                                -1 ->
                                    {delete, Key};
                                _ ->
                                    {ok, {Key, {{join, Max}, {remove, Max}}}}
                            end
                    end
            end,

            lock_update('Incomplete', Key, F)
    end,

    remove(Key, ?LEVEL_MAX - 1).

remove(Key, -1) ->
    ets:delete('Peer', Key);
remove(Key, Level) ->
    Result = gen_server:call(?MODULE,
        {Key,
            {'remove-process-1',
                {Key}},
            Level}),

    case Result of
        {already} ->
            {error, 'already-removing'};

        {ok, removed} ->
            update(Key, {'__none__', '__none__'}, Level),

            F = fun([{Key, {{join, _N}, {remove, Level}}}]) ->
                    {ok, {Key, {{join, _N}, {remove, Level - 1}}}}
            end,
            lock_update('Incomplete', Key, F),

            remove(Key, Level - 1)
    end.


%%--------------------------------------------------------------------
%% Function: put
%% Description(ja): 値をputする
%% Description(en): put value
%% Returns: ok | {error, Reason}
%%--------------------------------------------------------------------
put(Key, Value) ->
    join(Key, Value).


%%--------------------------------------------------------------------
%% Function: get
%% Description(ja): 値をgetする
%% Description(en): get value
%% Returns: {ok, [{Key, Value}, ...]} | {error, Reason}
%%--------------------------------------------------------------------
get(Key) ->
    gen_server:call(?MODULE, {get, Key, Key}).

get(Key0, Key1) when Key1 < Key0 ->
    get(Key1, Key0);
get(Key0, Key1) ->
    gen_server:call(?MODULE, {get, Key0, Key1}).


test() ->
    io:format("~nPeers = ~p~n", [ets:tab2list('Peer')]).


get_server() ->
    whereis(?MODULE).


get_peer() ->
    ets:tab2list('Peer').

