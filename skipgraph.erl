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

-module(skipgraph).
-behaviour(gen_server).
-export([start/1, init/1, handle_call/3, terminate/2,
        handle_cast/2, handle_info/2, code_change/3,
        join/1, join/2, test/0, get/1, get/2, put/2,
        get_server/0]).
-export([make_membership_vector/0]).

%-define(LEVEL_MAX, 8).
%-define(LEVEL_MAX, 16).
-define(LEVEL_MAX, 32).
%-define(LEVEL_MAX, 64).
%-define(LEVEL_MAX, 128).


%%--------------------------------------------------------------------
%% Function: start
%% Description(ja): 初期化を行い，gen_serverを起動する．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
start(Initial) ->
    % {Key, {Value, MembershipVector, Neighbor}}
    %   MembershipVector : <<1, 0, 1, 1, ...>>
    %   Neighbor : {Smaller, Bigger}
    %     Smaller : [{Node, Key}, ...]
    %     Smaller : [{Node, Key}, ...]
    ets:new('Peer', [ordered_set, public, named_table]),
    ets:new('Lock-Update-Daemon', [set, public, named_table]),
    ets:new('Incomplete', [set, public, named_table]),

    case Initial of
        '__none__' ->
            %gen_server:start_link({local, ?MODULE}, ?MODULE, ['__none__', make_membership_vector()], [{debug, [trace, log]}]);
            gen_server:start_link({local, ?MODULE}, ?MODULE, ['__none__', make_membership_vector()], []);

        _ ->
            InitialNode = rpc:call(Initial, skipgraph, get_server, []),
            %gen_server:start_link({local, ?MODULE}, ?MODULE, [InitialNode, make_membership_vector()], [{debug, [trace, log]}])
            gen_server:start_link({local, ?MODULE}, ?MODULE, [InitialNode, make_membership_vector()], [])
    end.


%%--------------------------------------------------------------------
%% Function: init
%% Description(ja): gen_server:start_linkにより呼び出される．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
init(Arg) ->
    {ok, Arg}.



%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: handle_call <peer>
%% Description(ja): ネットワーク先のノードから呼び出される．適当なピアを
%%                  呼び出し元に返す．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({peer, random}, _From, State) ->
    [{SelfKey, {_, _, _}} | _] = ets:tab2list('Peer'),
    {reply, {whereis(?MODULE), SelfKey}, State};


%%--------------------------------------------------------------------
%% Function: handle_call <put>
%% Description(ja): ピア(SelfKey)のValueを書き換える．join処理と組み合わせて使用．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {put, Value}}, _From, State) ->
    F = fun({_SelfKey, {_Value, MembershipVector, Neighbor}}) ->
            {ok, {SelfKey, {Value, MembershipVector, Neighbor}}}
    end,

    lock_update(SelfKey, F),
    {reply, ok, State};


%%--------------------------------------------------------------------
%% Function: handle_call <get>
%% Description(ja): getメッセージを受信し、適当なローカルピア(無ければ
%%                  グローバルピア)を選択して，get_process_0に繋げる．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({get, Key0, Key1}, From, [InitialNode | Tail]) ->
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


%%--------------------------------------------------------------------
%% Function: handle_call <get-process-0>
%% Description(ja): 最適なピアを探索する．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {'get-process-0', {Key0, Key1, From}}}, _From, State) ->
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

                % 最適なピアの探索を続ける(get-process-0)
                {BestNode, BestKey} ->
                    gen_server:call(BestNode,
                        {BestKey,
                            {'get-process-0',
                                {Key0, Key1, From}}})
            end
    end,

    spawn(F),
    {noreply, State};


%%--------------------------------------------------------------------
%% Function: handle_call <get-process-1>
%% Description(ja): 指定された範囲内を走査し，値を収集する．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {'get-process-1', {Key0, Key1, From}, ItemList}}, _From, State) ->
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


%%--------------------------------------------------------------------
%% Function: handle_call <join>
%% Description(ja): joinメッセージを受信し、適当なローカルピア(無ければ
%%                  グローバルピア)を返す．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({join, NewKey}, From, [InitialNode | Tail]) ->
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


%%--------------------------------------------------------------------
%% Function: handle_call <join-process-0> [0]
%% Description(ja): join-process-0にFromを渡して再度呼ぶ．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% Function: handle_call <join-process-0> [1]
%% Description(ja): 最もNewKeyに近いピアを(内側に向かって)探索する．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State)
when Level == 0 andalso SelfKey == NewKey ->

%    Self = whereis(?MODULE),

%    case ets:lookup('Incomplete', SelfKey) of
%        [{SelfKey, -1}] when From /= Self ->
%            io:format("~n~nrecall~n~n"),
%
%            timer:sleep(1),
%            spawn(fun() ->
%                        gen_server:call(?MODULE,
%                            {SelfKey,
%                                {'join-process-0',
%                                    {From,
%                                        Server,
%                                        NewKey,
%                                        MembershipVector}},
%                                Level})
%                end);
%        _ ->
            gen_server:reply(From, {exist, {whereis(?MODULE), SelfKey}}),
%    end,

    {noreply, State};


handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State)
when Level > 0 andalso SelfKey == NewKey ->

    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, -1}] ->
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
    end,

    {noreply, State};


handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, -1}] ->
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
                        [{SelfKey, MaxLevel}] when MaxLevel + 1 == Level ->
                            lists:nth(MaxLevel + 1, Neighbor);
                        [{SelfKey, MaxLevel}] when MaxLevel < Level ->
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
                            {BestNode, BestKey} = case ets:lookup('Incomplete', SelfKey) of
                                [{SelfKey, MaxLevel_}] when (MaxLevel_ + 1) == Level ->
                                    select_best(lists:nthtail(MaxLevel_, Neighbor), NewKey, S_or_B);
                                [{SelfKey, MaxLevel_}] when MaxLevel_ < Level ->
                                    % バグが無ければ，このパターンになることはない
                                    error;
                                _ ->
                                    select_best(lists:nthtail(Level - 1, Neighbor), NewKey, S_or_B)
                            end,
                            %io:format("BestKey=~p~n", [BestKey]),

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
    end,

    {noreply, State};


%%--------------------------------------------------------------------
%% Function: handle_call <join-process-1>
%% Description(ja): ネットワーク先ノードから呼び出されるために存在する．
%%                  join_process_1を，他の処理をlockして呼び出す．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {'join-process-1', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    join_process_1(SelfKey, {From, Server, NewKey, MembershipVector}, Level),
    {reply, '__none__', State};


%%--------------------------------------------------------------------
%% Function: handle_call <join-process-0-oneway> [0]
%% Description(ja): join-process-0-onewayにFromを渡して再度呼び出す．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% Function: handle_call <join-process-0-oneway> [1]
%% Description(ja): 基本はhandle_call(join-process-0)と同じだが，
%%                  join_process_1_oneway/3関数を呼び出す
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State)
when Level == 0 andalso SelfKey == NewKey ->

%    Self = whereis(?MODULE),
%
%    case ets:lookup('Incomplete', SelfKey) of
%        [{SelfKey, -1}] when From /= Self ->
%            io:format("~n~nrecall~n~n"),
%
%            timer:sleep(1),
%            spawn(fun() ->
%                        gen_server:call(?MODULE,
%                            {SelfKey,
%                                {'join-process-0-oneway',
%                                    {From,
%                                        Server,
%                                        NewKey,
%                                        MembershipVector}},
%                                Level})
%                end);
%
%        _ ->
            gen_server:reply(From, {exist, {whereis(?MODULE), SelfKey}}),
%    end,

    {noreply, State};


handle_call({SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State)
when Level > 0 andalso SelfKey == NewKey ->

    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, -1}] ->
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
    end,

    {noreply, State};


handle_call({SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    case ets:lookup('Incomplete', SelfKey) of
        [{SelfKey, -1}] ->
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
                        [{SelfKey, MaxLevel}] when MaxLevel + 1 == Level ->
                            % MaxLevel + 1 == Levelより，lists:nth(MaxLevel + 1, Neighbor) == lists:nth(Level, Neigbor)
                            % なのであまり意味は無い
                            lists:nth(MaxLevel + 1, Neighbor);

                        [{SelfKey, MaxLevel}] when MaxLevel < Level ->
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
                            %io:format("~nSelfKey=~p, NewKey=~p~nLevel=~p, S_or_B=~p~nNeighbor=~p~n", [SelfKey, NewKey, Level, S_or_B, Neighbor]),
                            %io:format("Peer(NewKey)=~p~n", [ets:lookup('Peer', NewKey)]),
                            {BestNode, BestKey} = case ets:lookup('Incomplete', SelfKey) of
                                [{SelfKey, MaxLevel_}] when (MaxLevel_ + 1) == Level ->
                                    select_best(lists:nthtail(MaxLevel_, Neighbor), NewKey, S_or_B);
                                [{SelfKey, MaxLevel_}] when MaxLevel_ < Level ->
                                    % バグが無ければ，このパターンになることはない
                                    error;
                                _ ->
                                    select_best(lists:nthtail(Level - 1, Neighbor), NewKey, S_or_B)
                            end,

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
    end,

    {noreply, State};


%%--------------------------------------------------------------------
%% Function: handle_call <join-process-1-oneway>
%% Description(ja): join_process_1_oneway/3関数を呼び出す．ローカルから
%%                  このコールバック関数を呼び出すことはほとんどない．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {'join-process-1-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    join_process_1_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level),
    {reply, '__none__', State};


%%--------------------------------------------------------------------
%% Function: handle_call <join-process-2>
%% Description(ja): ローカルで呼び出すことはない．Neighborをupdateする．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_call({SelfKey, {'join-process-2', {From, Server, NewKey}}, Level, Other},
    _From,
    State) ->

    update(SelfKey, {Server, NewKey}, Level),
    if
        NewKey < SelfKey ->
            gen_server:reply(From,
                {ok,
                    Other,
                    {whereis(?MODULE), SelfKey}});
        SelfKey < NewKey ->
            gen_server:reply(From,
                {ok,
                    {whereis(?MODULE), SelfKey},
                    Other})
    end,

    {reply, '__none__', State}.


%%--------------------------------------------------------------------
%% Function: join_process_1
%% Description(ja): handle_call<join-process-0>から呼び出される．
%%                  MembershipVector[Level]が一致するピアを外側に向かって探索．
%%                  成功したら自身のNeighborをupdateし，Anotherにもupdateメッセージを送信する．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
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
                [{SelfKey, MaxLevel}] when MaxLevel < Level ->
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
                            %spawn(fun() ->
                            %            gen_server:call(OtherNode,
                            %                {OtherKey,
                            %                    {'join-process-1',
                            %                        {From,
                            %                            Server,
                            %                            NewKey,
                            %                            MembershipVector}},
                            %                    Level})
                            %    end)
                        _ ->
                            spawn(fun() ->
                                        io:format("~n~n__incomplete_error__ SelfKey=~p, NewKey=~p, Level=~p~n~n", [SelfKey, NewKey, Level]),
                                        timer:sleep(2),
                                        gen_server:call(?MODULE,
                                            {SelfKey,
                                                {'join-process-0',
                                                    {From,
                                                        Server,
                                                        NewKey,
                                                        MembershipVector}},
                                                Level})
                                end)
                    end;

                % update処理を行い，反対側のピアにもメッセージを送信する
                _ ->
                    {Neighbor, Reply} = if
                        NewKey < SelfKey ->
                            {Smaller, {ok, {'__none__', '__none__'}, {whereis(?MODULE), SelfKey}}};
                        SelfKey < NewKey ->
                            {Bigger, {ok, {whereis(?MODULE), SelfKey}, {'__none__', '__none__'}}}
                    end,

                    case lists:nth(Level + 1, Neighbor) of
                        {'__none__', '__none__'} ->
                            io:format("update0, SelfKey=~p, OtherKey=~p, NewKey=~p~n", [SelfKey, {'__none__', '__none__'}, NewKey]),
                            update(SelfKey, {Server, NewKey}, Level),
                            gen_server:reply(From, Reply);

                        {OtherNode, OtherKey} ->
                            Self = whereis(?MODULE),
                            case OtherNode of
                                % 自分自身の場合，直接update関数を呼び出す
                                Self ->
                                    io:format("update1, SelfKey=~p, OtherKey=~p, NewKey=~p~n", [SelfKey, OtherKey, NewKey]),
                                    update(SelfKey, {Server, NewKey}, Level),
                                    update(OtherKey, {Server, NewKey}, Level),
                                    if
                                        NewKey < SelfKey ->
                                            gen_server:reply(From, {ok, {Self, OtherKey}, {Self, SelfKey}});
                                        SelfKey < NewKey ->
                                            gen_server:reply(From, {ok, {Self, SelfKey}, {Self, OtherKey}})
                                    end;

                                _ ->
                                    gen_server:call(OtherNode,
                                        {OtherKey,
                                            {'join-process-2',
                                                {From,
                                                    Server,
                                                    NewKey}},
                                            Level,
                                            {whereis(?MODULE), SelfKey}}),
                                    update(SelfKey, {Server, NewKey}, Level)
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


%%--------------------------------------------------------------------
%% Function: join_process_1_oneway
%% Description(ja): handle_call<join-process-0-oneway>から呼び出される．
%%                  基本はjoin_process_1/3関数と同じだが，片方向にしか
%%                  探索しない．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
join_process_1_oneway(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->

    io:format("join_process_1_oneway: SelfKey=~p, NewKey=~p, Level=~p~n", [SelfKey, NewKey, Level]),
    [{SelfKey, {_, SelfMembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    TailN = ?LEVEL_MAX - Level - 1,
    <<_:TailN, Bit:1, _:Level>> = MembershipVector,
    <<_:TailN, SelfBit:1, _:Level>> = SelfMembershipVector,

    case Bit of
        SelfBit ->
            case ets:lookup('Incomplete', SelfKey) of
                [{SelfKey, MaxLevel}] when MaxLevel < Level ->
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
                            %spawn(fun() ->
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
                            spawn(fun() ->
                                        io:format("~n~n__incomplete_error__ SelfKey=~p, NewKey=~p, Level=~p~n~n", [SelfKey, NewKey, Level]),
                                        timer:sleep(2),
                                        gen_server:call(?MODULE,
                                            {SelfKey,
                                                {'join-process-0-oneway',
                                                    {From,
                                                        Server,
                                                        NewKey,
                                                        MembershipVector}},
                                                Level})
                                end)
                    end;

                _ ->
                    update(SelfKey, {Server, NewKey}, Level),
                    gen_server:reply(From,
                        {ok, {whereis(?MODULE), SelfKey}})
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


%%--------------------------------------------------------------------
%% Function: terminate
%% Description(ja): gen_server内でエラーが発生したとき，再起動させる．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
    spawn(fun() ->
                test(),
                unregister(?MODULE),
                gen_server:start_link({local, ?MODULE}, ?MODULE, State, [{debug, [trace, log]}])
        end),
    ok.


%%--------------------------------------------------------------------
%% Function: handle_cast
%% Description(ja): 何もしない
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_cast(_Message, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_info
%% Description(ja): 何もしない
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: code_change
%% Description(ja): 何もしない
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _NewVsn) ->
    {ok, State}.



%%====================================================================
%% Utilities
%%====================================================================

%%--------------------------------------------------------------------
%% Function: lock_update
%% Description(ja): 任意のキーに対してlock_update_daemonプロセスを生成
%%                  し，ロックされたupdateを行う
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
lock_update(Key, Func) ->
    lock_update('Peer', Key, Func).

lock_update(Table, Key, Func) ->
    Ref = make_ref(),
    case ets:lookup('Lock-Update-Daemon', Key) of
        [] ->
            ets:insert('Lock-Update-Daemon', {Key, spawn(fun lock_update_daemon/0)}),
            [{Key, Daemon}] = ets:lookup('Lock-Update-Daemon', Key),
            Daemon ! {{self(), Ref}, {update, Table, {Key, Func}}};

        [{Key, Daemon}] ->
            Daemon ! {{self(), Ref}, {update, Table, {Key, Func}}}
    end,

    receive
        {Ref, ok} -> ok
    end.


%%--------------------------------------------------------------------
%% Function: lock_update_daemon
%% Description(ja): updateが並列に実行された際に競合を防ぐための排他的
%%                  処理．キー毎に独立したプロセスとして常駐する．
%% Description(en): 
%% Returns:
%%--------------------------------------------------------------------
lock_update_daemon() ->
    receive
        {{From, Ref}, {update, Table, {Key, Func}}} ->
            [Item] = ets:lookup(Table, Key),

            case Func(Item) of
                {ok, Result} ->
                    ets:insert(Table, Result);
                {error, _Reason} ->
                    pass
            end,
            From ! {Ref, ok}
    end,

    lock_update_daemon().

%%--------------------------------------------------------------------
%% Function: update
%% Description(ja): Neighbor[Level]を更新する
%% Description(en): update Neighbor[Level]
%% Returns:
%%--------------------------------------------------------------------
update(SelfKey, {Server, NewKey}, Level) ->
    F = fun({_, {Value, MembershipVector, {Smaller, Bigger}}}) ->
            io:format("NewKey=~p  SelfKey=~p~n", [NewKey, SelfKey]),
            if
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


%%--------------------------------------------------------------------
%% Function: select_best
%% Description(ja): Neighborの中から最適なピアを選択する
%% Description(en): select the best peer from a neighbor
%% Returns: {'__none__', '__none__'} | {'__self__'} | {Node, Key}
%%--------------------------------------------------------------------
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


%%--------------------------------------------------------------------
%% Function: make_membership_vector
%% Description(ja): MembershipVectorを生成する
%% Description(en): make membership vector
%% Returns: <<1:1, N:1, M:1, ...>>
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% Function: join
%% Description(ja): 新しいピアをjoinする
%% Description(en): join a new peer
%% Returns: ok | {error, Reason}
%%--------------------------------------------------------------------
join(Key) ->
    join(Key, '__none__').

join(Key, Value) ->
    case ets:lookup('Peer', Key) of
        [{Key, {_, _, _}}] ->
            F = fun({SelfKey, {_Value, MembershipVector, Neighbor}}) ->
                    {ok, {SelfKey, {Value, MembershipVector, Neighbor}}}
            end,

            lock_update(Key, F),
            ok;

        [] ->
            MembershipVector = make_membership_vector(),
            Neighbor = {lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'}),
                lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'})},

            case gen_server:call(whereis(?MODULE), {join, Key}) of
                {ok, {'__none__', '__none__'}} ->
                    ets:insert('Peer', {Key, {Value, MembershipVector, Neighbor}}),
                    ets:insert('Lock-Update-Daemon', {Key, spawn(fun lock_update_daemon/0)}),
                    ok;

                {ok, InitPeer} ->
                    ets:insert('Peer', {Key, {Value, MembershipVector, Neighbor}}),
                    ets:insert('Lock-Update-Daemon', {Key, spawn(fun lock_update_daemon/0)}),
                    ets:insert('Incomplete', {Key, -1}),
                    join(InitPeer, Key, Value, MembershipVector)
            end
    end.

join(InitPeer, Key, Value, MembershipVector) ->
    join(InitPeer, Key, Value, MembershipVector, 0, {'__none__', '__none__'}).

join(_, _, _, _, ?LEVEL_MAX, _) ->
    ok;
join({InitNode, InitKey}, NewKey, Value, MembershipVector, Level, OtherPeer) ->
    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-0',
                {whereis(?MODULE),
                    NewKey,
                    MembershipVector}},
            Level}),

    %io:format("NewKey=~p, Level=~p, Result=~p~n", [NewKey, Level, Result]),

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
                    ets:delete('Peer', Key),
                    ets:delete('Incomplete', Key),
                    gen_server:call(Node, {Key, {put, Value}})
            end,
            ok;

        {ok, {'__none__', '__none__'}, {'__none__', '__none__'}} ->
            ets:delete('Incomplete', NewKey),
            ok;

        {ok, {'__none__', '__none__'}, BiggerPeer} ->
            update(NewKey, BiggerPeer, Level),

            F = fun({Key, _MaxLevel}) ->
                    {ok, {Key, Level}}
            end,
            lock_update('Incomplete', NewKey, F),

            join_oneway(BiggerPeer, NewKey, Value, MembershipVector, Level + 1);

        {ok, SmallerPeer, {'__none__', '__none__'}} ->
            update(NewKey, SmallerPeer, Level),

            F = fun({Key, _MaxLevel}) ->
                    {ok, {Key, Level}}
            end,
            lock_update('Incomplete', NewKey, F),

            join_oneway(SmallerPeer, NewKey, Value, MembershipVector, Level + 1);

        {ok, SmallerPeer, BiggerPeer} ->
            update(NewKey, SmallerPeer, Level),
            update(NewKey, BiggerPeer, Level),

            F = fun({Key, _MaxLevel}) ->
                    {ok, {Key, Level}}
            end,
            lock_update('Incomplete', NewKey, F),

            join(SmallerPeer, NewKey, Value, MembershipVector, Level + 1, BiggerPeer);

        {error, mismatch} ->
            case OtherPeer of
                {'__none__', '__none__'} ->
                    ets:delete('Incomplete', NewKey),
                    ok;
                _ ->
                    join_oneway(OtherPeer, NewKey, Value, MembershipVector, Level)
            end;

        Message ->
            io:format("*ERR* join/5:unknown message: ~p~n", [Message]),
            error
    end.


%%--------------------------------------------------------------------
%% Function: join_oneway
%% Description(ja): 一方のNeighborが'__none__'のとき，もう一方のNeighborを
%%                  通して新たなピアをjoinする
%% Description(en): when a neighbor is '__none__', this function join a
%%                  new peer through only an another
%% Returns: ok | {error, Reason}
%%--------------------------------------------------------------------
join_oneway(_, _, _, _, ?LEVEL_MAX) ->
    ok;
join_oneway({InitNode, InitKey}, NewKey, Value, MembershipVector, Level) ->
    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-0-oneway',
                {whereis(?MODULE),
                    NewKey,
                    MembershipVector}},
            Level}),

    %io:format("NewKey=~p, Level=~p, Result=~p~n", [NewKey, Level, Result]),

    case Result of
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
                    ets:delete('Peer', Key),
                    ets:delete('Incomplete', Key),
                    gen_server:call(Node, {Key, {put, Value}})
            end,
            ok;

        {ok, {'__none__', '__none__'}} ->
            ets:delete('Incomplete', NewKey),
            ok;

        {ok, Peer} ->
            update(NewKey, Peer, Level),

            F = fun({Key, _MaxLevel}) ->
                    {ok, {Key, Level}}
            end,
            lock_update('Incomplete', NewKey, F),

            join_oneway(Peer, NewKey, Value, MembershipVector, Level + 1);

        {error, mismatch} ->
            ets:delete('Incomplete', NewKey),
            ok;

        Message ->
            io:format("*ERR* join/4:unknown message: ~p~n", [Message]),
            error
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


%%--------------------------------------------------------------------
%% Function: test
%% Description(ja): システムが持つ全てのピア情報を示す
%% Description(en): show all peers which the system has
%% Returns:
%%--------------------------------------------------------------------
test() ->
    io:format("~nPeers = ~p~n", [ets:tab2list('Peer')]).


%%--------------------------------------------------------------------
%% Function: get
%% Description(ja): サーバのpidを返す
%% Description(en): return pid of server
%% Returns: pid()
%%--------------------------------------------------------------------
get_server() ->
    whereis(?MODULE).

