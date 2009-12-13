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
        join/1, join/2, test/0, get/1, get/2, put/2,
        get_server/0]).

-define(LEVEL_MAX, 8).


start(Initial) ->
    % {Key, {Value, MembershipVector, Neighbor}}
    %   MembershipVector : <<1, 0, 1, 1, ...>>
    %   Neighbor : {Smaller, Bigger}
    %     Smaller : [{Node, Key}, ...]
    %     Smaller : [{Node, Key}, ...]
    ets:new('Peer', [ordered_set, public, named_table]),

    case Initial of
        '__none__' ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, ['__none__', make_membership_vector()], [{debug, [trace, log]}]);

        _ ->
            InitialNode = rpc:call(Initial, skipgraph, get_server, []),
            gen_server:start_link({local, ?MODULE}, ?MODULE, [InitialNode, make_membership_vector()], [{debug, [trace, log]}])
    end.


init(Arg) ->
    {ok, Arg}.


handle_call({peer, random}, _From, State) ->
    [{SelfKey, {_, _, _}} | _] = ets:tab2list('Peer'),
    {reply, {whereis(?MODULE), SelfKey}, State};


% Combine with join
handle_call({SelfKey, {put, Value}}, _From, State) ->
    [{SelfKey, {_Value, MembershipVector, Neighbor}}] = ets:lookup('Peer', SelfKey),
    ets:insert('Peer', {SelfKey, {Value, MembershipVector, Neighbor}}),

    {reply, ok, State};


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


% joinメッセージを受信し、適当なローカルピア(無ければグローバルピア)を返す
handle_call({join, NewKey}, From, [InitialNode | Tail]) ->
    F = fun() ->
            PeerList = ets:tab2list('Peer'),
            case PeerList of
                [] ->
                    case InitialNode of
                        '__none__' ->
                            io:format("PeerList=~p~n", [PeerList]),
                            gen_server:reply(From,
                                {ok, {'__none__', '__none__'}});
                        _ ->
                            {_, Key} = gen_server:call(InitialNode, {peer, random}),
                            gen_server:reply(From, {ok, {InitialNode, Key}})
                    end;

                _ ->
                    [{SelfKey, {_, _, _}} | _] = PeerList,
                    gen_server:reply(From, {ok, {whereis(?MODULE), SelfKey}})
            end
    end,

    spawn(F),
    {noreply, [InitialNode | Tail]};


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


% 最もNewKeyに近いピアを(内側に向かって)探索する

handle_call({SelfKey, {'join-process-0', {From, _Server, NewKey, _MembershipVector}}, _Level},
    _From,
    State)
when SelfKey == NewKey ->
 
    gen_server:reply(From, {exist, {whereis(?MODULE), SelfKey}}),
    {noreply, State};

handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {Neighbor, S_or_B} = if
        NewKey < SelfKey ->
            {Smaller, smaller};
        SelfKey < NewKey ->
            {Bigger, bigger}
    end,

    case select_best(lists:nthtail(Level, Neighbor), NewKey, S_or_B) of
        % 最適なピアが見つかったので、次のフェーズ(join_process_1)へ移行
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
    end,

    {noreply, State};


handle_call({SelfKey, {'join-process-1', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    join_process_1(SelfKey, {From, Server, NewKey, MembershipVector}, Level),
    {reply, '__none__', State};


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

    {reply, '__none__', State};


handle_call({SelfKey, {'join-process-oneway-0', {Server, NewKey, MembershipVector}}, Level},
    From,
    State) ->

    spawn(fun() ->
                gen_server:call(whereis(?MODULE),
                    {SelfKey,
                        {'join-process-oneway-0',
                            {From,
                                Server,
                                NewKey,
                                MembershipVector}},
                        Level})
        end),
    {noreply, State};


% 最もNewKeyに近いピアを(内側に向かって)探索する
 
handle_call({SelfKey, {'join-process-oneway-0', {From, _Server, NewKey, _MembershipVector}}, _Level},
    _From,
    State)
when NewKey == SelfKey ->

    gen_server:reply(From, {exist, {whereis(?MODULE), SelfKey}}),
    {noreply, State};

handle_call({SelfKey, {'join-process-oneway-0', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {Neighbor, S_or_B} = if
        NewKey < SelfKey ->
            {Smaller, smaller};
        SelfKey < NewKey ->
            {Bigger, bigger}
    end,
 
    case select_best(lists:nthtail(Level, Neighbor), NewKey, S_or_B) of
        % 最適なピアが見つかったので、次のフェーズ(join_process_1)へ移行
        {'__none__', '__none__'} ->
            join_process_oneway_1(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level);
        {'__self__'} ->
            join_process_oneway_1(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level);

        % 最適なピアの探索を続ける(join-process-oneway-0)
        {BestNode, BestKey} ->
            spawn(fun() ->
                        gen_server:call(BestNode,
                            {BestKey,
                                {'join-process-oneway-0',
                                    {From,
                                        Server,
                                        NewKey,
                                        MembershipVector}},
                                Level})
                end)
    end,

    {noreply, State};


handle_call({SelfKey, {'join-process-oneway-1', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    join_process_oneway_1(SelfKey, {From, Server, NewKey, MembershipVector}, Level),
    {reply, '__none__', State};


handle_call(Message, _From, State) ->
    io:format("*ERR* Not support message: ~p~n", [Message]),
    {noreply, State}.


terminate(_Reason, State) ->
    spawn(fun() ->
                unregister(?MODULE),
                gen_server:start_link({local, ?MODULE}, ?MODULE, State, [{debug, [trace, log]}])
        end),
    ok.


% MembershipVector[Level]が一致するピアを外側に向かって探索．
% 成功したら自身のNeighborをupdateし，Anotherにもupdateメッセージを送信する．
join_process_1(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->

    [{SelfKey, {_, SelfMembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    TailN = ?LEVEL_MAX - Level - 1,
    <<_:Level, Bit:1, _:TailN>> = MembershipVector,
    <<_:Level, SelfBit:1, _:TailN>> = SelfMembershipVector,

    case Bit of
        SelfBit ->
            if
                NewKey < SelfKey ->
                    case lists:nth(Level + 1, Smaller) of
                        {'__none__', '__none__'} ->
                            update(SelfKey, {Server, NewKey}, Level),

                            gen_server:reply(From,
                                {ok,
                                    {'__none__', '__none__'},
                                    {whereis(?MODULE), SelfKey}});

                        {OtherNode, OtherKey} ->
                            Self = whereis(?MODULE),
                            case OtherNode of
                                Self ->
                                    spawn(fun() ->
                                                gen_server:call(OtherNode,
                                                    {OtherKey,
                                                        {'join-process-2',
                                                            {From,
                                                                Server,
                                                                NewKey}},
                                                        Level,
                                                        {whereis(?MODULE), SelfKey}})
                                        end);
                                _ ->
                                    gen_server:call(OtherNode,
                                        {OtherKey,
                                            {'join-process-2',
                                                {From,
                                                    Server,
                                                    NewKey}},
                                            Level,
                                            {whereis(?MODULE), SelfKey}})
                            end,
                            update(SelfKey, {Server, NewKey}, Level)
                    end;

                SelfKey < NewKey ->
                    case lists:nth(Level + 1, Bigger) of
                        {'__none__', '__none__'} ->
                            update(SelfKey, {Server, NewKey}, Level),

                            gen_server:reply(From,
                                {ok,
                                    {whereis(?MODULE), SelfKey},
                                    {'__none__', '__none__'}});

                        {OtherNode, OtherKey} ->
                            Self = whereis(?MODULE),
                            case OtherNode of
                                Self ->
                                    spawn(fun() ->
                                                gen_server:call(OtherNode,
                                                    {OtherKey,
                                                        {'join-process-2',
                                                            {From,
                                                                Server,
                                                                NewKey}},
                                                        Level,
                                                        {whereis(?MODULE), SelfKey}})
                                        end);
                                _ ->
                                    gen_server:call(OtherNode,
                                        {OtherKey,
                                            {'join-process-2',
                                                {From,
                                                    Server,
                                                    NewKey}},
                                            Level,
                                            {whereis(?MODULE), SelfKey}})
                            end,
                            update(SelfKey, {Server, NewKey}, Level)
                    end
            end;

        _ ->
            if
                NewKey < SelfKey ->
                    case lists:nth(Level + 1, Bigger) of
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
                    end;

                SelfKey < NewKey ->
                    case lists:nth(Level + 1, Smaller) of
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
            end
    end.


% MembershipVector[Level]が一致するピアを外側に向かって探索．
% 成功したら自身のNeighborをupdateし，Anotherにもupdateメッセージを送信する．
join_process_oneway_1(SelfKey, {From, Server, NewKey, MembershipVector}, Level) ->

    [{SelfKey, {_, SelfMembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    TailN = ?LEVEL_MAX - Level - 1,
    <<_:Level, Bit:1, _:TailN>> = MembershipVector,
    <<_:Level, SelfBit:1, _:TailN>> = SelfMembershipVector,

    case Bit of
        SelfBit ->
            update(SelfKey, {Server, NewKey}, Level),
            gen_server:reply(From,
                {ok, {whereis(?MODULE), SelfKey}});

        _ ->
            if
                NewKey < SelfKey ->
                    case lists:nth(Level + 1, Bigger) of
                        {'__none__', '__none__'} ->
                            gen_server:reply(From, {error, mismatch});
                        {NextNode, NextKey} ->
                            spawn(fun() ->
                                        gen_server:call(NextNode,
                                            {NextKey,
                                                {'join-process-oneway-1',
                                                    {From,
                                                        Server,
                                                        NewKey,
                                                        MembershipVector}},
                                                Level})
                                end)
                    end;

                SelfKey < NewKey ->
                    case lists:nth(Level + 1, Smaller) of
                        {'__none__', '__none__'} ->
                            gen_server:reply(From, {error, mismatch});
                        {NextNode, NextKey} ->
                            spawn(fun() ->
                                        gen_server:call(NextNode,
                                            {NextKey,
                                                {'join-process-oneway-1',
                                                    {From,
                                                        Server,
                                                        NewKey,
                                                        MembershipVector}},
                                                Level})
                                end)
                    end
            end
    end.


update(SelfKey, {Server, NewKey}, Level) ->
    [{SelfKey, {Value, MembershipVector, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),
    if
        NewKey < SelfKey ->
            {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Smaller),

            NewSmaller = Front ++ [{Server, NewKey} | Tail],
            ets:insert('Peer', {SelfKey, {Value, MembershipVector, {NewSmaller, Bigger}}});

        SelfKey < NewKey ->
            {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Bigger),

            NewBigger = Front ++ [{Server, NewKey} | Tail],
            ets:insert('Peer', {SelfKey, {Value, MembershipVector, {Smaller, NewBigger}}})
    end.


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


make_membership_vector() ->
    make_membership_vector(<<1:1, (random:uniform(256) - 1):7>>, ?LEVEL_MAX / 8 - 1).

make_membership_vector(Bin, 0.0) ->
    Bin;
make_membership_vector(Bin, N) ->
    make_membership_vector(<<Bin/binary, (random:uniform(256) - 1):8>>, N - 1).


join(Key) ->
    join(Key, '__none__').

join(Key, Value) ->
    MembershipVector = make_membership_vector(),
    Neighbor = {lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'}),
                lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'})},

    case gen_server:call(whereis(?MODULE), {join, Key}) of
        {ok, {'__none__', '__none__'}} ->
            ets:insert('Peer', {Key, {Value, MembershipVector, Neighbor}}),
            ok;

        {ok, InitPeer} ->
            ets:insert('Peer', {Key, {Value, MembershipVector, Neighbor}}),
            join(InitPeer, Key, Value, MembershipVector)
    end.

join(InitPeer, Key, Value, MembershipVector) ->
    join(InitPeer, Key, Value, MembershipVector, 0, {'__none__', '__none__'}).

join(_, _, _, _, ?LEVEL_MAX, _) ->
    ok;
join({InitNode, InitKey}, NewKey, Value, MembershipVector, N, OtherPeer) ->
    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-0',
                {whereis(?MODULE),
                    NewKey,
                    MembershipVector}},
            N}),

    case Result of
        {exist, {Node, Key}} ->
            gen_server:call(Node, {Key, {put, Value}}),
            ok;

        {ok, {'__none__', '__none__'}, {'__none__', '__none__'}} ->
            ok;

        {ok, {'__none__', '__none__'}, BiggerPeer} ->
            update(NewKey, BiggerPeer, N),
            join_oneway(BiggerPeer, NewKey, Value, MembershipVector, N + 1);

        {ok, SmallerPeer, {'__none__', '__none__'}} ->
            update(NewKey, SmallerPeer, N),
            join_oneway(SmallerPeer, NewKey, Value, MembershipVector, N + 1);

        {ok, SmallerPeer, BiggerPeer} ->
            update(NewKey, SmallerPeer, N),
            update(NewKey, BiggerPeer, N),
            join(SmallerPeer, NewKey, Value, MembershipVector, N + 1, BiggerPeer);

        {error, mismatch} ->
            case OtherPeer of
                {'__none__', '__none__'} ->
                    ok;
                _ ->
                    join_oneway(OtherPeer, NewKey, Value, MembershipVector, N)
            end;

        Message ->
            io:format("*ERR* join/5:unknown message: ~p~n", [Message]),
            error
    end.


join_oneway(_, _, _, _, ?LEVEL_MAX) ->
    ok;
join_oneway({InitNode, InitKey}, NewKey, Value, MembershipVector, N) ->
    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-oneway-0',
                {whereis(?MODULE),
                    NewKey,
                    MembershipVector}},
            N}),

    case Result of
        {exist, {Node, Key}} ->
            gen_server:call(Node, {Key, {put, Value}}),
            ok;

        {ok, {'__none__', '__none__'}} ->
            ok;

        {ok, Peer} ->
            update(NewKey, Peer, N),
            join_oneway(Peer, NewKey, Value, MembershipVector, N + 1);

        {error, mismatch} ->
            ok;

        Message ->
            io:format("*ERR* join/4:unknown message: ~p~n", [Message]),
            error
    end.


put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}).


get(Key) ->
    gen_server:call(?MODULE, {get, Key, Key}).

get(Key0, Key1) when Key1 < Key0 ->
    get(Key1, Key0);
get(Key0, Key1) ->
    gen_server:call(?MODULE, {get, Key0, Key1}).


get_server() ->
    whereis(?MODULE).


test() ->
    io:format("~nPeers = ~p~n", [ets:tab2list('Peer')]).
