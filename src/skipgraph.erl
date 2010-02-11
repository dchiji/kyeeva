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

%%    TODO
%%
%%      * {join, NewKey}で，適当なピアの選択時にNewKeyを有効活用する
%%      * join中にサーバが再起動した場合の処理
%%      * get時に死んだノードを発見した場合の処理

-module(skipgraph).
-behaviour(gen_server).

-export([start/1, init/1, handle_call/3, terminate/2,
        handle_cast/2, handle_info/2, code_change/3,
        join/1, join/2, remove/1, test/0, get/1, get/2, get/3, put/2]).

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
            %        util:make_membership_vector()],
            %    [{debug, [trace, log]}]);
            gen_server:start_link(
                {local, ?MODULE},
                ?MODULE,
                ['__none__',
                    util:make_membership_vector()],
                []);

        _ ->
            InitialNode = rpc:call(Initial, skipgraph, get_server, []),
            %gen_server:start_link(
            %    {local, ?MODULE},
            %    ?MODULE,
            %    [InitialNode,
            %        util:make_membership_vector()],
            %    [{debug, [trace, log]}])
            gen_server:start_link(
                {local, ?MODULE},
                ?MODULE,
                [InitialNode,
                    util:make_membership_vector()],
                [])
    end.



%%====================================================================
%% gen_server callbacks
%%====================================================================

%% gen_server:start_linkにより呼び出される．
init(Arg) ->
    % [{{Type, Value}, UniqueKey, MembershipVector, Neighbor}},
    %  {{Type, Value}, UniqueKey, ...}},
    %  ...]
    %
    %   Type: atom()
    %   MembershipVector: byte()
    %   Neighbor: {Smaller, Bigger}
    %     Smaller: [{Node, Key}, ...]
    %     Smaller: [{Node, Key}, ...]
    T = ets:new('Peer', [set, public, named_table]),
    ets:new('Types', [set, public, named_table]),

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
    {reply, {self(), SelfKey}, State};


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
handle_call({{Type, Key}, {put, UniqueKey}}, _From, State) ->
    F = fun([{{Type, Key}, {UniqueList, MembershipVector, Neighbor}}]) ->
            {ok, {{Type, Key}, {[UniqueKey | UniqueList], MembershipVector, Neighbor}}}
    end,

    util:lock_update({Type, Key}, F),
    {reply, ok, State};


%% lookupメッセージを受信し、適当なローカルピア(無ければグローバルピア)を選択して，lookup_process_0に繋げる．
handle_call({lookup, Key0, Key1, TypeList, From},
    _From,
    [InitialNode | Tail]) ->

    Ref = make_ref(),

    spawn(fun() ->
                lookup:lookup(InitialNode, Key0, Key1, TypeList, {Ref, From})
        end),
    {reply, Ref, [InitialNode | Tail]};


%% 最適なピアを探索する．
handle_call({SelfKey, {'lookup-process-0', {Key0, Key1, TypeList, From}}},
    _From,
    State) ->

    spawn(fun() ->
                lookup:process_0(SelfKey, Key0, Key1, TypeList, From)
        end),
    {reply, '__none__', State};


%% 指定された範囲内を走査し，値を収集する．
handle_call({SelfKey, {'lookup-process-1', {Key0, Key1, TypeList, From}}},
    _From,
    State) ->

    spawn(fun() ->
                lookup:process_1(SelfKey, Key0, Key1, TypeList, From)
        end),
    {noreply, State};


%% joinメッセージを受信し、適当なローカルピア(無ければグローバルピア)を返す．
handle_call({'select-first-peer', _NewKey},
    From,
    [InitialNode | Tail]) ->

    PeerList = ets:tab2list('Peer'),
    Self = self(),

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
            gen_server:reply(From, {ok, {Self, SelfKey}})
    end,

    {noreply, [InitialNode | Tail]};


%% join-process-0にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'join-process-0', {Server, NewKey, MembershipVector}}, Level},
    From,
    State) ->

    Self = self(),

    spawn(fun() ->
                gen_server:call(Self,
                    {SelfKey,
                        {'join-process-0',
                            {From,
                                Server,
                                NewKey,
                                MembershipVector}},
                        Level})
        end),

    {noreply, State};

%% join:process_0/3関数をutil:lock_joinに与える
handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join:process_0(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    util:lock_join(SelfKey, F),
    {noreply, State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% join:process_1を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'join-process-1', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join:process_1(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    util:lock_join(SelfKey, F),
    {reply, '__none__', State};


%% join-process-0-onewayにFromを渡して再度呼び出す．
handle_call({SelfKey, {'join-process-0-oneway', {Server, NewKey, MembershipVector}}, Level},
    From,
    State) ->

    Self = self(),

    spawn(fun() ->
                gen_server:call(Self,
                    {SelfKey,
                        {'join-process-0-oneway',
                            {From,
                                Server,
                                NewKey,
                                MembershipVector}},
                        Level})
        end),

    {noreply, State};

%% join:process_0_oneway/3関数をutil:lock_joinに与える
handle_call({SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join:process_0_oneway(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    util:lock_join(SelfKey, F),
    {noreply, State};


%% join:process_1_oneway/3関数を呼び出す．ローカルからこのコールバック関数を呼び出すことはほとんどない．
handle_call({SelfKey, {'join-process-1-oneway', {From, Server, NewKey, MembershipVector}}, Level},
    _From,
    State) ->

    F = fun() ->
            join:process_1_oneway(SelfKey,
                {From,
                    Server,
                    NewKey,
                    MembershipVector},
                Level)
    end,

    util:lock_join(SelfKey, F),
    {reply, '__none__', State};


%% ローカルで呼び出すことはない．Neighborをupdateする．
handle_call({SelfKey, {'join-process-2', {From, Server, NewKey}}, Level, Other},
    _From,
    State) ->

    Self = self(),

    F = fun() ->
            Ref = make_ref(),
            util:update(SelfKey, {Server, NewKey}, Level),

            if
                NewKey < SelfKey ->
                    gen_server:reply(From,
                        {ok,
                            {Other,
                                {Self, SelfKey}},
                            {self(), Ref}});

                SelfKey < NewKey ->
                    gen_server:reply(From,
                        {ok,
                            {{Self, SelfKey},
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

    util:lock_join(SelfKey, F),
    {reply, '__none__', State};


%% remove-process-0にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'remove-process-0', {RemovedKey}}, Level},
    From,
    State) ->

    Self = self(),

    spawn(fun() ->
                gen_server:call(Self,
                    {SelfKey,
                        {'remove-process-0',
                            {From,
                                RemovedKey}},
                        Level})
        end),

    {noreply, State};

%% remove:process_0/3関数をutil:lock_joinに与える
handle_call({SelfKey, {'remove-process-0', {From, RemovedKey}}, Level},
    _From,
    State) ->

    F = fun() ->
            remove:process_0(SelfKey,
                {From,
                    RemovedKey},
                Level)
    end,

    util:lock_join(SelfKey, F),
    {reply, '__none__', State};


%% remove-process-1にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'remove-process-1', {RemovedKey}}, Level},
    From,
    State) ->

    Self = self(),

    spawn(fun() ->
                gen_server:call(Self,
                    {SelfKey,
                        {'remove-process-1',
                            {From,
                                RemovedKey}},
                        Level})
        end),

    {noreply, State};

%% ネットワーク先ノードから呼び出されるために存在する．
%% remove:process_1を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-1', {From, RemovedKey}}, Level},
    _From,
    State) ->

    F = fun() ->
            remove:process_1(SelfKey,
                {From,
                    RemovedKey},
                Level)
    end,

    util:lock_join(SelfKey, F),
    {reply, '__none__', State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% remove:process_2を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-2', {From, RemovedKey}}, NewNeighbor, Level},
    _From,
    State) ->

    F = fun() ->
            remove:process_2(SelfKey,
                {From,
                    RemovedKey},
                NewNeighbor,
                Level)
    end,

    util:lock_join(SelfKey, F),
    {reply, '__none__', State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% remove:process_3を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-3', {From, RemovedKey}}, NewNeighbor, Level},
    _From,
    State) ->

    remove:process_3(SelfKey, {From, RemovedKey}, NewNeighbor, Level),
    {reply, '__none__', State}.


%% gen_server内でエラーが発生したとき，再起動させる．
%% 今のところ正常に動作しないが，デバッグなどに便利なので絶賛放置中
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
%% Interfaces
%%====================================================================


%% 新しいピアをjoinする

join(Key) ->
    join(Key, '__none__').

join(UniqueKey, {Type, Key}) ->
    case ets:lookup('Peer', {Type, Key}) of
        [{{Type, Key}, {_, _, _}}] ->
            F = fun([{{Type, Key}, {UniqueList, MembershipVector, Neighbor}}]) ->
                    {ok, {{Type, Key}, {[UniqueKey | UniqueList], MembershipVector, Neighbor}}}
            end,

            util:lock_update({Type, Key}, F);

        [] ->
            MembershipVector = util:make_membership_vector(),
            Neighbor = {lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'}),
                lists:duplicate(?LEVEL_MAX, {'__none__', '__none__'})},

            InitTables = fun() ->
                    ets:insert('Peer', {{Type, Key}, {UniqueKey, MembershipVector, Neighbor}}),

                    ets:insert('Lock-Join-Daemon',
                        {{Type, Key},
                            spawn(fun() ->
                                        util:lock_daemon(fun util:lock_join_callback/1)
                                end)}),
                    ets:insert('Lock-Update-Daemon',
                        {{Type, Key},
                            spawn(fun() ->
                                        util:lock_daemon(fun util:lock_update_callback/1)
                                end)}),

                    ets:insert('Types', {UniqueKey, Type}),
                    ets:insert('Types', {{UniqueKey, Type}, Key})
            end,

            case gen_server:call(whereis(?MODULE), {'select-first-peer', {Type, Key}}) of
                {ok, {'__none__', '__none__'}} ->
                    InitTables();

                {ok, InitPeer} ->
                    ets:insert('Incomplete', {{Type, Key}, {{join, -1}, {remove, ?LEVEL_MAX}}}),
                    InitTables(),

                    join(InitPeer, {Type, Key}, UniqueKey, MembershipVector)
            end
    end,
    ok.


join_delete_if_exist(Node, Key, Value, TableList) ->
    Self = whereis(?MODULE),

    case Node of
        Self ->
            case ets:lookup('Incomplete', Key) of
                [{Key, {{join, -1}, {remove, _}}}] ->
                    ets:delete('Incomplete', Key),
                    gen_server:call(Node, {Key, {put, Value}});
                [{Key, _}] ->
                    gen_server:call(Node, {Key, {put, Value}})
            end;
        _ ->
            lists:foreach(fun(Table) ->
                        ets:delete(Table, Key)
                end,
                TableList),
            gen_server:call(Node, {Key, {put, Value}})
    end.


join(InitPeer, Key, Value, MembershipVector) ->
    join(InitPeer, Key, Value, MembershipVector, 0, {'__none__', '__none__'}).

join(_, Key, _, _, ?LEVEL_MAX, _) ->
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
    util:lock_update('Incomplete', Key, F);

join({InitNode, InitKey}, NewKey, Value, MembershipVector, Level, OtherPeer) ->
    Daemon = spawn(fun() -> util:wait_trap([]) end),
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
            join_delete_if_exist(Node, Key, Value, ['Incomplete', 'Lock-Join-Daemon', 'Lock-Update-Daemon', 'Peer']);

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
            util:lock_update('Incomplete', NewKey, F);

        {ok, {{'__none__', '__none__'}, BiggerPeer}, {Pid, Ref}} ->
            io:format("~nBiggerPeer=~p~n", [BiggerPeer]),

            util:update(NewKey, BiggerPeer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case util:lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join_oneway(BiggerPeer, NewKey, Value, MembershipVector, Level + 1);

        {ok, {SmallerPeer, {'__none__', '__none__'}}, {Pid, Ref}} ->
            io:format("~nSmallerPeer=~p~n", [SmallerPeer]),

            util:update(NewKey, SmallerPeer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case util:lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join_oneway(SmallerPeer, NewKey, Value, MembershipVector, Level + 1);

        {ok, {SmallerPeer, BiggerPeer}, {Pid, Ref}} ->
            io:format("~nSmallerPeer=~p, BiggerPeer=~p, {~p, ~p}~n", [SmallerPeer, BiggerPeer, Pid, Ref]),

            util:update(NewKey, SmallerPeer, Level),
            util:update(NewKey, BiggerPeer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case util:lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join(SmallerPeer, NewKey, Value, MembershipVector, Level + 1, BiggerPeer);

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
                    util:lock_update('Incomplete', NewKey, F);

                _ ->
                    join_oneway(OtherPeer, NewKey, Value, MembershipVector, Level)
            end;

        Message ->
            io:format("*ERR* join/5:unknown message: ~p~n", [Message]),
            error
    end.


%% 一方のNeighborが'__none__'のとき，もう一方のNeighborを通して新たなピアをjoinする

join_oneway(_, Key, _, _, ?LEVEL_MAX) ->
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
    util:lock_update('Incomplete', Key, F);

join_oneway({InitNode, InitKey}, NewKey, Value, MembershipVector, Level) ->
    Daemon = spawn(fun() -> util:wait_trap([]) end),
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
            join_delete_if_exist(Node, Key, Value, ['Peer', 'Incomplete']);

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
            util:lock_update('Incomplete', NewKey, F);

        {ok, Peer, {Pid, Ref}} ->
            util:update(NewKey, Peer, Level),

            F = fun([{Key, {{join, JLevel}, {remove, RLevel}}}]) ->
                    case RLevel of
                        ?LEVEL_MAX ->
                            {ok, {Key, {{join, Level}, {remove, RLevel}}}};

                        _ ->
                            {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
                    end
            end,

            case util:lock_update('Incomplete', NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    Daemon ! {error, removing};

                _ ->
                    Pid ! {ok, Ref},
                    Daemon ! {trap}
            end,

            join_oneway(Peer, NewKey, Value, MembershipVector, Level + 1);

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
            util:lock_update('Incomplete', NewKey, F);

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

            util:lock_update('Incomplete', Key, F)
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
            util:update(Key, {'__none__', '__none__'}, Level),

            F = fun([{Key, {{join, _N}, {remove, Level}}}]) ->
                    {ok, {Key, {{join, _N}, {remove, Level - 1}}}}
            end,
            util:lock_update('Incomplete', Key, F),

            remove(Key, Level - 1)
    end.


%%--------------------------------------------------------------------
%% Function: put
%% Description(ja): 値をputする
%% Description(en): put value
%% Returns: ok | {error, Reason}
%%--------------------------------------------------------------------
put(UniqueKey, KeyList) ->
    F = fun(Key) ->
            join(UniqueKey, Key)
    end,

    ets:insert('Types', {UniqueKey, [Type || {Type, _} <- KeyList]}),
    join(UniqueKey, {'unique-key', UniqueKey}),

    lists:foreach(F, KeyList).


%%--------------------------------------------------------------------
%% Function: get
%% Description(ja): 値をgetする
%% Description(en): get value
%% Returns: {ok, [{Key, Value}, ...]} | {error, Reason}
%%--------------------------------------------------------------------
get(Key) ->
    get(Key, [value]).

get(Key, TypeList) ->
    lookup:call(?MODULE, Key, Key, TypeList).

get(Key0, Key1, TypeList) when Key1 < Key0 ->
    get(Key1, Key0, TypeList);
get(Key0, Key1, TypeList) ->
    lookup:call(?MODULE, Key0, Key1, TypeList).


test() ->
    io:format("~nPeers = ~p~n", [ets:tab2list('Peer')]).


get_server() ->
    whereis(?MODULE).


get_peer() ->
    ets:tab2list('Peer').

