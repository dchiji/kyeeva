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

-module(sg).

-behaviour(gen_server).

-include("../include/common.hrl").

%% API
-export([get_server/0,
        get_peer/0,
        join/1,
        join/2,
        remove/1,
        test/0,
        get/1,
        get/2,
        get/3,
        put/2]).

%% gen_server cacllbacks
-export([start/1,
        init/1,
        handle_call/3,
        terminate/2,
        handle_cast/2,
        handle_info/2,
        code_change/3]).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).


start(Initial) ->
    case Initial of
        nil ->
            gen_server:start_link( {local, ?MODULE}, ?MODULE, [nil, util:make_membership_vector()], [{debug, [trace, log]}]);
        _ ->
            InitialNode = rpc:call(Initial, ?MODULE, get_server, []),
            gen_server:start_link({local, ?MODULE}, ?MODULE, [InitialNode, util:make_membership_vector()], [{debug, [trace, log]}])
    end.


%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Arg) ->
    PeerTable = ets:new(peer_table, [ordered_set, public, named_table]),
    ets:new('Types', [set, public, named_table]),
    ets:new(incomplete_table, [set, public, named_table]),
    ets:new(node_ets_table, [set, public, named_table]),
    ets:insert(node_ets_table, {?MODULE, PeerTable}),

    util_lock:lock_init(),
    util_lock:wakeup_init(),

    {ok, Arg}.


handle_call({peer, random}, _From, State) ->
    [{SelfKey, {_, _}} | _] = ets:tab2list(peer_table),
    {reply, {self(), SelfKey}, State};


handle_call({'get-ets-table'}, _From, State) ->
    [{?MODULE, Tab}] = ets:lookup(node_ets_table, ?MODULE),
    {reply, Tab, State};


%% ピア(SelfKey)のValueを書き換える．join処理と組み合わせて使用．
handle_call({Key, {put, UniqueKey}}, _From, State) ->
    %%
    %% todo
    %%
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
    {reply, nil, State};


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

    PeerList = ets:tab2list(peer_table),
    Self = self(),

    case PeerList of
        [] ->
            case InitialNode of
                nil ->
                    gen_server:reply(From,
                        {ok, {nil, nil}});
                _ ->
                    {_, Key} = gen_server:call(InitialNode, {peer, random}),
                    gen_server:reply(From, {ok, {InitialNode, Key}})
            end;

        _ ->
            [{SelfKey, {_, _}} | _] = PeerList,
            gen_server:reply(From, {ok, {Self, SelfKey}})
    end,

    {noreply, [InitialNode | Tail]};


%% join-process-0にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'join-process-0', {Server, NewKey, MVector}}, Level}, From, State) ->
    spawn(gen_server, call, [self(), {SelfKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level}]),
    {noreply, State};

%% join:process_0/3関数をutil_lock:join_lockに与える
handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() ->
            join:process_0(SelfKey, {From, Server, NewKey, MVector}, Level)
    end,
    util_lock:join_lock({SelfKey, Level}, F),
    {noreply, State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% join:process_1を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'join-process-1', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() ->
            join:process_1(SelfKey, {From, Server, NewKey, MVector}, Level)
    end,
    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};


%% join-process-0-onewayにFromを渡して再度呼び出す．
handle_call({SelfKey, {'join-process-0-oneway', {Server, NewKey, MVector}}, Level}, From, State) ->
    spawn(gen_server, call, [self(), {SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MVector}}, Level}]),
    {noreply, State};

%% join:process_0_oneway/3関数をutil_lock:join_lockに与える
handle_call({SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() ->
            join:process_0_oneway(SelfKey, From, Server, NewKey, MVector, Level)
    end,
    util_lock:join_lock({SelfKey, Level}, F),
    {noreply, State};


%% join:process_1_oneway/3関数を呼び出す．ローカルからこのコールバック関数を呼び出すことはほとんどない．
handle_call({SelfKey, {'join-process-1-oneway', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() ->
            join:process_1_oneway(SelfKey, From, Server, NewKey, MVector, Level)
    end,
    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};


%% ローカルで呼び出すことはない．Neighborをupdateする．
handle_call({SelfKey, {'join-process-2', {From, Server, NewKey}}, Level, Other}, _From, State) ->
    Self = self(),
    F = fun() ->
            Ref = make_ref(),
            util:update(SelfKey, {Server, NewKey}, Level),
            if
                NewKey < SelfKey -> gen_server:reply(From, {ok, {Other, {Self, SelfKey}}, {self(), Ref}});
                SelfKey < NewKey -> gen_server:reply(From, {ok, {{Self, SelfKey}, Other}, {self(), Ref}})
            end,
            % reply先がNeighborの更新に成功するまで待機
            receive
                {ok, Ref} ->
                    ok
            end,
            case ets:lookup(node_ets_table, Server) of
                [] -> spawn(ets, insert, [node_ets_table, {Server, gen_server:call(Server, {'get-ets-table'})}]);
                _  -> ok
            end
    end,
    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};


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

%% remove:process_0/3関数をutil_lock:join_lockに与える
handle_call({SelfKey, {'remove-process-0', {From, RemovedKey}}, Level},
    _From,
    State) ->

    F = fun() ->
            remove:process_0(SelfKey,
                {From,
                    RemovedKey},
                Level)
    end,

    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};


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

    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};


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

    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};


%% ネットワーク先ノードから呼び出されるために存在する．
%% remove:process_3を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-3', {From, RemovedKey}}, NewNeighbor, Level},
    _From,
    State) ->

    remove:process_3(SelfKey, {From, RemovedKey}, NewNeighbor, Level),
    {reply, nil, State}.


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
    join(Key, nil).

join(UniqueKey, {Type, Key}) ->
    InitTables = fun() ->
            ets:insert(peer_table, {{{Type, Key}, UniqueKey}, {MVector, Neighbor}}),
            ets:insert('Types', {UniqueKey, Type}),
            ets:insert('Types', {{UniqueKey, Type}, Key})
    end,

    case ets:lookup(peer_table, {{Type, Key}, UniqueKey}) of
        [{{{Type, Key}, UniqueKey}, {_, _}}] -> pass;
        [] ->
            MVector    = util:make_membership_vector(),
            Neighbor   = {lists:duplicate(?LEVEL_MAX, {nil, nil}), lists:duplicate(?LEVEL_MAX, {nil, nil})},
            case gen_server:call(whereis(?MODULE), {'select-first-peer', {{Type, Key}, UniqueKey}}) of
                {ok, {nil, nil}} -> InitTables();
                {ok, InitPeer} ->
                    ets:insert(incomplete_table, {{{Type, Key}, UniqueKey}, {{join, -1}, {remove, ?LEVEL_MAX}}}),
                    InitTables(),
                    join(InitPeer, {{Type, Key}, UniqueKey}, MVector)
            end
    end,
    ok.


join(InitPeer, Key, MVector) ->
    join(InitPeer, Key, MVector, 0, {nil, nil}).

join(_, Key, _, ?LEVEL_MAX, _) ->
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
    util:ets_lock(peer_table, incomplete_table, Key, F);

join({InitNode, InitKey}, NewKey, MVector, Level, OtherPeer) ->
    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-0',
                {whereis(?MODULE),
                    NewKey,
                    MVector}},
            Level},
        ?TIMEOUT),

    case Result of
        {exist, {Node, Key}} ->
            pass;

        {ok, {{nil, nil}, {nil, nil}}, _} ->
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
            util:ets_lock(peer_table, incomplete_table, NewKey, F);

        {ok, {{nil, nil}, BiggerPeer}, {Pid, Ref}} ->
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

            case util:ets_lock(peer_table, incomplete_table, NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    util_lock:wakeup_with_report({NewKey, Level}, removing);
                _ ->
                    Pid ! {ok, Ref},
                    util_lock:wakeup({NewKey, Level})
            end,

            join_oneway(BiggerPeer, NewKey, MVector, Level + 1);

        {ok, {SmallerPeer, {nil, nil}}, {Pid, Ref}} ->
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

            case util:ets_lock(peer_table, incomplete_table, NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    util_lock:wakeup_with_report({NewKey, Level}, removing);
                _ ->
                    Pid ! {ok, Ref},
                    util_lock:wakeup({NewKey, Level})
            end,

            join_oneway(SmallerPeer, NewKey, MVector, Level + 1);

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

            case util:ets_lock(peer_table, incomplete_table, NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    util_lock:wakeup_with_report({NewKey, Level}, removing);
                _ ->
                    Pid ! {ok, Ref},
                    util_lock:wakeup({NewKey, Level})
            end,

            join(SmallerPeer, NewKey, MVector, Level + 1, BiggerPeer);

        {error, mismatch} ->
            case OtherPeer of
                {nil, nil} ->
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
                    util:ets_lock(peer_tableincomplete_table, NewKey, F);

                _ ->
                    join_oneway(OtherPeer, NewKey, MVector, Level)
            end;

        Message ->
            io:format("*ERR* join/5:unknown message: ~p~n", [Message]),
            error
    end.


%% 一方のNeighborがnilのとき，もう一方のNeighborを通して新たなピアをjoinする

join_oneway(_, Key, _, ?LEVEL_MAX) ->
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
    util:ets_lock(peer_table, incomplete_table, Key, F);

join_oneway({InitNode, InitKey}, NewKey,  MVector, Level) ->
    Result = gen_server:call(InitNode,
        {InitKey,
            {'join-process-0-oneway',
                {whereis(?MODULE),
                    NewKey,
                    MVector}},
            Level},
        ?TIMEOUT),

    %io:format("NewKey=~p, Level=~p, Result=~p~n", [NewKey, Level, Result]),

    case Result of
        {exist, {Node, Key}} ->
            pass;

        {ok, {nil, nil}, _} ->
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
            util:ets_lock(peer_table, incomplete_table, NewKey, F);

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

            case util:ets_lock(peer_table, incomplete_table, NewKey, F) of
                {ok, {Key, {{join, Level}, {remove, RLevel}}}} when RLevel /= ?LEVEL_MAX ->
                    Pid ! {error, Ref, removing},
                    util_lock:wakeup_with_report({NewKey, Level}, removing);
                _ ->
                    Pid ! {ok, Ref},
                    util_lock:wakeup({NewKey, Level})
            end,

            join_oneway(Peer, NewKey, MVector, Level + 1);

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
            util:ets_lock(peer_table, incomplete_table, NewKey, F);

        Message ->
            io:format("*ERR* join/4:unknown message: ~p~n", [Message]),
            error
    end.


remove(Key) ->
    case ets:lookup(peer_table, Key) of
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

            util:ets_lock(peer_table, incomplete_table, Key, F)
    end,

    remove(Key, ?LEVEL_MAX - 1).

remove(Key, -1) ->
    ets:delete(peer_table, Key);
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
            util:update(Key, {nil, nil}, Level),

            F = fun([{Key, {{join, _N}, {remove, Level}}}]) ->
                    {ok, {Key, {{join, _N}, {remove, Level - 1}}}}
            end,
            util:ets_lock(peer_table, incomplete_table, Key, F),

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
    join(undefined, {'unique-key', UniqueKey}),

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
    io:format("~nPeers = ~p~n", [ets:tab2list(peer_table)]).


get_server() ->
    whereis(?MODULE).


get_peer() ->
    ets:tab2list(peer_table).
