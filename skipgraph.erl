-module(skipgraph).
-export([start/1, init/1, handle_call/3, join/1]).
-behaviour(gen_server).

-define(LEVEL_MAX, 8).
-define(NOW, fun(Term) -> io:format("test:now:\t=>   ~p~n", [Term]) end).

select_best([], _Key) ->
    {'__none__', '__none__'};
select_best([Item], _Key) ->
    Item;
select_best([{'__none__', '__none__'} | _], _Key) ->
    {'__none__', '__none__'};
select_best([{Node0, Key0}, {'__none__', '__none__'} | _], _Key) ->
    {Node0, Key0};
select_best([{Node0, Key0}, {Node1, Key1} | Tail], Key) when Key1 < Key0 ->    % smaller and smaller
    if
        Key0 < Key ->
            {'__self__'};
        Key1 < Key ->
            {Node0, Key0};
        Key < Key0 ->
            select_best([{Node1, Key1} | Tail], Key)
    end;
select_best([{Node0, Key0}, {Node1, Key1} | Tail], Key) when Key0 < Key1 ->    % bigger and bigger
    if
        Key < Key0 ->
            {'__self__'};
        Key < Key1 ->
            {Node0, Key0};
        Key0 < Key ->
            select_best([{Node1, Key1} | Tail], Key)
    end.

start(InitialNode) ->
    % {Key, {Value, MembershipVector, Neighbor}}
    %   MembershipVector : <<1, 0, 1, 1, ...>>
    %   Neighbor : [{Node, Key}, ...]
    %     Node=local : '__local__'
    ets:new('Peer', [ordered_set, public, named_table]),
    ets:new('Incomplete', [bag, public, named_table]),
    ets:new('Property', [set, public, named_table]),

    {ok, Server} = gen_server:start_link({global, manager}, ?MODULE, [InitialNode, make_membership_vector()], []),
    ets:insert('Property', {server, Server}),

    {ok, Server}.

init(Arg) ->
    {ok, Arg}.

% joinメッセージを受信し、適当なローカルピアにjoin-process-0メッセージを送信する
handle_call({join, Ref, NewKey, MembershipVector},
    From,
    [InitialNode | Tail]) ->

    SelfServer = self(),
    F = fun() ->
            PeerList = ets:tab2list('Peer'),

            case PeerList of
                [] ->
                    case InitialNode of
                        '__none__' ->
                            NewSmaller = NewBigger = lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'}),
                            gen_server:reply(From, {NewSmaller, NewBigger});
                        _ ->
                            {_, Key} = gen_server:call(InitialNode, {peer, random}),
                            gen_server:call(InitialNode,
                                {Key,
                                    {'join-process-0',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector}})
                    end;

                _ ->
                    [{SelfKey, {_, _, _}} | _] = PeerList,
                    gen_server:call(SelfServer,
                        {SelfKey,
                            {'join-process-0',
                                {From, Ref},
                                NewKey,
                                MembershipVector}})
            end
    end,

    spawn(F),
    {noreply, [InitialNode | Tail]};

% 最もNewKeyに近いピアを探索する
handle_call({SelfKey, {'join-process-0', {From, Ref}, NewKey, MembershipVector}},
    _From,
    [InitialNode | Tail]) ->

    ?NOW('join-process-0'),

    SelfServer = self(),
    F = fun() ->
            [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

            if
                % HEAD--A--B--NewKey-(-C--D-)-SelfKey--E--F--TAIL
                NewKey < SelfKey ->
                    case select_best(Smaller, NewKey) of
                        % 最適なピアが見つかったので、次のフェーズ(join-process-1)へ移行
                        {'__none__', '__none__'} ->
                            Next = {'__none__', '__none__'},
                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-1',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        0,
                                        []}, 
                                    Next});
                        {'__self__'} ->
                            Next = lists:nth(0, Smaller),
                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-1',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        0,
                                        []}},
                                Next);

                        % 最適なピアの探索を続ける(join-process-0)
                        {'__local__', BestKey} ->
                            gen_server:call(SelfServer,
                                {BestKey,
                                    {'join-process-0',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector}});
                        {Server, BestKey} ->
                            gen_server:call(Server,
                                {BestKey,
                                    {'join-process-0',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector}})
                    end;

                % HEAD--A--B--SelfKey-(-C--D-)-NewKey--E--F--TAIL
                SelfKey < NewKey ->
                    case select_best(Bigger, NewKey) of
                        % 最適なピアが見つかったので、次のフェーズ(join-process-1)へ移行
                        {'__none__', '__none__'} ->
                            Next = {'__none__', '__none__'},
                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-1',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        0,
                                        []},
                                    Next});
                        {'__self__'} ->
                            Next = lists:nth(0, Bigger),
                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-1',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        0,
                                        []},
                                    Next});

                        % 最適なピアの探索を続ける(join-process-0)
                        {'__local__', BestKey} ->
                            gen_server:call(SelfServer,
                                {BestKey,
                                    {'join-process-0',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector}});
                        {Server, BestKey} ->
                            gen_server:call(Server,
                                {BestKey,
                                    {'join-process-0',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector}})
                    end
            end
    end,

    spawn(F),
    {reply, '__none__', [InitialNode, Tail]};

% MembershipVector[Level]が一致するピアを外側に向かって探索 & LevelMaxに達したとき、join-process-1へ移行
% {最適なlocal-key, {'join-process-1', 新しくjoinするkey, keyのMembershipVector}}
handle_call({SelfKey, {'join-process-1', {From, Ref}, NewKey, MembershipVector, Level, Neighbor}, Next},
    _From,
    State) ->

    ?NOW('join-process-1'),

    SelfServer = self(),
    F = fun() ->
            [{SelfKey, {_, SelfMembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

            case Level of
                0 ->
                    if
                        NewKey < SelfKey ->
                            ets:insert('Incomplete', {{From, Ref, bigger}, Level}),

                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-1',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        Level + 1,
                                        [{SelfKey, SelfServer}]},
                                    Next});
                        SelfKey < NewKey ->
                            ets:insert('Incomplete', {{From, Ref, smaller}, Level}),

                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-1',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        Level + 1,
                                        [{SelfKey, SelfServer}]},
                                    Next})
                    end;

                ?LEVEL_MAX ->
                    ?NOW('join-process-1: LEVEL_MAX'),
                    {NextNode, NextKey} = Next,
                    case NextNode of
                        % join-process-1の初期ノードが設定されていなければ，そのまま結果を返す
                        '__none__' ->
                            AnotherNeighbor = lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'}),
                            if
                                NewKey < SelfKey -> gen_server:reply(From, {AnotherNeighbor, Neighbor});
                                SelfKey < NewKey -> gen_server:reply(From, {Neighbor, AnotherNeighbor})
                            end;
                        % それ以外のとき，join-process-2へ移行
                        _ ->
                            gen_server:call(NextNode,
                                {NextKey,
                                    {'join-process-2',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        0,
                                        Neighbor,
                                        []}})
                    end;

                _ ->
                    ?NOW('join-process-1: _'),
                    TailN = ?LEVEL_MAX - Level - 1,
                    <<_:Level, Bit:1, _:TailN>> = MembershipVector,
                    <<_:Level, SelfBit:1, _:TailN>> = SelfMembershipVector,

                    if
                        NewKey < SelfKey ->
                            case Bit of
                                SelfBit ->
                                    ets:insert('Incomplete', {{From, Ref, bigger}, Level}),

                                    gen_server:call(SelfServer,
                                        {SelfKey, 
                                            {'join-process-1',
                                                {From, Ref},
                                                NewKey,
                                                MembershipVector,
                                                Level + 1,
                                                [{SelfKey, SelfServer} | Neighbor]},
                                            Next});
                                _ ->
                                    {BiggerNode, BiggerKey} = lists:nth(Level, Bigger),
                                    Remnant = lists:duplicate(?LEVEL_MAX - Level, {'__none__', '__none__'}),
                                    case BiggerNode of
                                        '__none__' ->
                                            case Next of
                                                {'__none__', '__none__'} ->
                                                    gen_server:reply(From,
                                                        {lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'}),
                                                            [Remnant | Neighbor]});
                                                {NextNode, NextKey} ->
                                                    gen_server:call(NextNode,
                                                        {NextKey,
                                                            {'join-process-2',
                                                                {From, Ref},
                                                                NewKey,
                                                                MembershipVector,
                                                                0,
                                                                [Remnant | Neighbor],
                                                                []}})
                                            end;
                                        _ ->
                                            gen_server:call(BiggerNode,
                                                {BiggerKey,
                                                    {'join-process-1',
                                                        {From, Ref},
                                                        NewKey,
                                                        MembershipVector,
                                                        Level,
                                                        Neighbor},
                                                    Next})
                                    end
                            end;

                        SelfKey < NewKey ->
                            case Bit of
                                SelfBit ->
                                    ets:insert('Incomplete', {{From, Ref, smaller}, Level}),

                                    gen_server:call(SelfServer,
                                        {SelfKey,
                                            {'join-process-1',
                                                {From, Ref},
                                                NewKey,
                                                MembershipVector,
                                                Level + 1,
                                                [{SelfKey, SelfServer} | Neighbor]},
                                            Next});
                                _ ->
                                    {SmallerNode, SmallerKey} = lists:nth(Level, Smaller),
                                    Remnant = lists:duplicate(?LEVEL_MAX - Level, {'__none__', '__none__'}),
                                    case SmallerNode of
                                        '__none__' ->
                                            case Next of
                                                {'__none__', '__none__'} ->
                                                    gen_server:reply(From,
                                                        {[Remnant | Neighbor],
                                                            lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'})});
                                                {NextNode, NextKey} ->
                                                    gen_server:call(NextNode,
                                                        {NextKey,
                                                            {'join-process-2',
                                                                {From, Ref},
                                                                NewKey,
                                                                MembershipVector, 
                                                                0,
                                                                [Remnant | Neighbor],
                                                                []}})
                                            end;
                                        _ ->
                                            gen_server:call(SmallerNode,
                                                {SmallerKey,
                                                    {'join-process-1',
                                                        {From, Ref},
                                                        NewKey,
                                                        MembershipVector,
                                                        Level,
                                                        Neighbor},
                                                    Next})
                                    end
                            end
                    end
            end
    end,

    spawn(F),
    {reply, '__none__', State};

% join-process-1の反対向きに処理を行う
% {最適なlocal-key, {'join-process-2', 新しくjoinするkey, keyのMembershipVector}}
handle_call({SelfKey, {'join-process-2', {From, Ref}, NewKey, MembershipVector, Level, AnotherNeighbor, Neighbor}},
    _From,
    State) ->

    ?NOW('join-process-2'),

    SelfServer = self(),
    F = fun() ->
            [{SelfKey, {_, SelfMembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

            case Level of
                ?LEVEL_MAX ->
                    ?NOW('join-process-2: LEVEL_MAX'),
                    if
                        NewKey < SelfKey -> gen_server:reply(From, {Neighbor, AnotherNeighbor});
                        SelfKey < NewKey -> gen_server:reply(From, {AnotherNeighbor, Neighbor})
                    end;

                0 ->
                    ?NOW('join-process-2: 0'),
                    if
                        NewKey < SelfKey ->
                            ets:insert('Incomplete', {{From, Ref, bigger}, Level}),

                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-2',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        Level + 1,
                                        AnotherNeighbor,
                                        [{SelfKey, SelfServer}]}});
                        SelfKey < NewKey ->
                            ets:insert('Incomplete', {{From, Ref, smaller}, Level}),

                            gen_server:call(SelfServer,
                                {SelfKey,
                                    {'join-process-2',
                                        {From, Ref},
                                        NewKey,
                                        MembershipVector,
                                        Level + 1,
                                        AnotherNeighbor,
                                        [{SelfKey, SelfServer}]}})
                    end;

                _ ->
                    ?NOW('join-process-2: _'),
                    TailN = ?LEVEL_MAX - Level - 1,
                    <<_:Level, Bit:1, _:TailN>> = MembershipVector,
                    <<_:Level, SelfBit:1, _:TailN>> = SelfMembershipVector,

                    if
                        NewKey < SelfKey ->
                            case Bit of
                                SelfBit ->
                                    ets:insert('Incomplete', {{From, Ref, bigger}, Level}),

                                    gen_server:call(SelfServer,
                                        {SelfKey,
                                            {'join-process-2',
                                                {From, Ref},
                                                NewKey,
                                                MembershipVector,
                                                Level + 1,
                                                AnotherNeighbor,
                                                [{SelfKey, SelfServer} | Neighbor]}});
                                _ ->
                                    {BiggerNode, BiggerKey} = lists:nth(Level, Bigger),
                                    case BiggerNode of
                                        '__none__' ->
                                            Remnant = lists:duplicate(?LEVEL_MAX - Level, {'__none__', '__none__'}),
                                            gen_server:reply(From,
                                                {[Remnant | Neighbor],
                                                    AnotherNeighbor});
                                        _ ->
                                            gen_server:call(BiggerNode,
                                                {BiggerKey,
                                                    {'join-process-2',
                                                        {From, Ref},
                                                        NewKey,
                                                        MembershipVector,
                                                        Level,
                                                        AnotherNeighbor,
                                                        Neighbor}})
                                    end
                            end;

                        SelfKey < NewKey ->
                            case Bit of
                                SelfBit ->
                                    ets:insert('Incomplete', {{From, Ref, smaller}, Level}),

                                    gen_server:call(SelfServer,
                                        {SelfKey,
                                            {'join-process-2',
                                                {From, Ref},
                                                NewKey,
                                                MembershipVector,
                                                Level + 1,
                                                AnotherNeighbor, 
                                                [{SelfKey, SelfServer} | Neighbor]}});
                                _ ->
                                    {SmallerNode, SmallerKey} = lists:nth(Level, Smaller),
                                    case SmallerNode of
                                        '__none__' ->
                                            Remnant = lists:duplicate(?LEVEL_MAX - Level, {'__none__', '__none__'}),
                                            gen_server:reply(From,
                                                {AnotherNeighbor,
                                                    [Remnant | Neighbor]});
                                        _ ->
                                            gen_server:call(SmallerNode,
                                                {SmallerKey,
                                                    {'join-process-2',
                                                        {From, Ref},
                                                        NewKey,
                                                        MembershipVector,
                                                        Level,
                                                        AnotherNeighbor,
                                                        Neighbor}})
                                    end
                            end
                    end
            end
    end,

    spawn(F),
    {reply, '__none__', State};

handle_call({SelfKey, {'join-update', Ref, Server, NewKey}},
    From,
    State) ->
    ;

handle_call(Message, From, State) ->
    io:format("Not support message: ~p~n", [Message]),
    {noreply, State}.

make_membership_vector() ->
    make_membership_vector(<<>>, ?LEVEL_MAX / 8).

make_membership_vector(Bin, 0.0) ->
    Bin;
make_membership_vector(Bin, N) ->
    make_membership_vector(<<Bin/binary, (random:uniform(256) - 1):8>>, N - 1).

join(Key) ->
    [{server, Server}] = ets:lookup('Property', server),
    MembershipVector = make_membership_vector(),
    Ref = make_ref(),

    {Smaller, Bigger} = gen_server:call(Server, {join, Ref, Key, MembershipVector}, infinity),
    ets:insert('Peer', {Key,{'value', MembershipVector, {lists:reverse(Smaller), lists:reverse(Bigger)}}}).

putv(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}).

getv(Key) ->
    case gen_server:call(?MODULE, {get, Key}) of
        {ok, Value} ->
            Value;
        {error, 'Not Found', {Node, Key}} ->
            'Not Found'
    end.