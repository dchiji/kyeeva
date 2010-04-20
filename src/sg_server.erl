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

-module(sg_server).

-behaviour(gen_server).

-include("../include/common_sg.hrl").

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

%% for spawn/3
-export([init_tables/2]).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).


%%====================================================================
%% API
%%====================================================================
start(InitNode) ->
    case InitNode of
        nil -> gen_server:start_link({local, ?MODULE}, ?MODULE, [nil, util_mvector:make()], [{debug, [trace]}]);
        _ -> gen_server:start_link({local, ?MODULE}, ?MODULE, [rpc:call(InitNode, ?MODULE, get_server, []), util_mvector:make()], [{debug, [trace, log]}])
    end.


put(ParentKey, KeyList) ->
    ets:insert(attribute_table, {ParentKey, [Attribute || {Attribute, _} <- KeyList]}),
    lists:foreach(fun(Key) -> join(ParentKey, Key) end, KeyList).


get(Key) ->
    get(Key, [value]).

get(Key, AttributeList) ->
    sg_lookup:call(?MODULE, Key, Key, AttributeList).

get(Key0, Key1, AttributeList) when Key1 < Key0 ->
    get(Key1, Key0, AttributeList);
get(Key0, Key1, AttributeList) ->
    sg_lookup:call(?MODULE, Key0, Key1, AttributeList).


test() ->
    io:format("~nPeers = ~p~n", [ets:tab2list(peer_table)]),
    io:format("~n"),
    [util_mvector:io_binary(Pstate#pstate.mvector) || {_, Pstate} <- ets:tab2list(peer_table)],
    io:format("~n").


get_server() ->
    whereis(?MODULE).


get_peer() ->
    ets:tab2list(peer_table).


%% 新しいピアをjoinする
join(Key) ->
    join(nil, {nil, Key}).

join(ParentKey, {Attribute, Key}) ->
    InitTables = fun(MVector, {Smaller, Bigger}) ->
            NewPstate = #pstate {
                parent_key = ParentKey,
                mvector = MVector,
                smaller = Smaller,
                bigger = Bigger
            },
            ets:insert(peer_table, {{{Attribute, Key}, ParentKey}, NewPstate}),
            ets:insert(attribute_table, {ParentKey, Attribute}),
            ets:insert(attribute_table, {{ParentKey, Attribute}, Key})
    end,

    case ets:lookup(peer_table, {{Attribute, Key}, ParentKey}) of
        [{{{Attribute, Key}, ParentKey}, {_, _}}] -> pass;
        [] ->
            MVector    = util_mvector:make(),
            Neighbor   = {lists:duplicate(?LEVEL_MAX, {nil, nil}), lists:duplicate(?LEVEL_MAX, {nil, nil})},
            case gen_server:call(whereis(?MODULE), {'select-first-peer', {{Attribute, Key}, ParentKey}}) of
                {ok, {nil, nil}} -> InitTables(MVector, Neighbor);
                {ok, InitPeer} ->
                    ets:insert(incomplete_table, {{{Attribute, Key}, ParentKey}, {{join, -1}, {remove, -1}}}),
                    InitTables(MVector, Neighbor),
                    join(InitPeer, {{Attribute, Key}, ParentKey}, MVector)
            end
    end,
    ok.


join(InitPeer, Key, MVector) ->
    join(InitPeer, Key, MVector, 1, {nil, nil}).

join({nil, nil}, _, _, _, _) ->
    ok;
join(_, Key, _, ?LEVEL_MAX, _) ->
    DeleteIncompleteElementCallback = fun
        ([])                                           -> {pass};
        ([{Key1, {_, {remove, -1}}}])          -> {delete, Key1};
        ([{Key1, {{join, JLevel}, {remove, RLevel}}}]) -> {ok, {Key1, {{join, JLevel}, {remove, RLevel}}}}
    end,
    util_lock:ets_lock(incomplete_table, Key, DeleteIncompleteElementCallback);
join({InitServer, InitKey}, NewKey, MVector, Level, OtherPeer) ->
    DeleteIncompleteElementCallback = fun
        ([])                                          -> {pass};
        ([{Key, {_, {remove, -1}}}])          -> {delete, Key};
        ([{Key, {{join, JLevel}, {remove, RLevel}}}]) -> {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
    end,
    IncrLevelCallback = fun
        ([{Key, {{join, _}, {remove, -1}}}]) -> {ok, {Key, {{join, Level}, {remove, -1}}}};
        ([{Key, {{join, JLevel}, {remove, RLevel}}}])     -> {ok, {Key, {{join, JLevel}, {remove, RLevel}}}}
    end,
    IncrLevel = fun() -> util_lock:ets_lock(incomplete_table, NewKey, IncrLevelCallback) end,
    Reply = fun
        ({ok, {_, {_, {remove, -1}}}}, Pid, Ref) ->
            io:format("pid=~p, ref=~p~n", [Pid, Ref]),
            Pid ! {ok, Ref},
            util_lock:wakeup({NewKey, Level});
        ({ok, _}, Pid, Ref) ->
            Pid ! {error, Ref, removing},
            util_lock:wakeup_with_report({NewKey, Level}, removing)
    end,
    JoinProcess = fun
        ({nil, nil}, {nil, nil}, _, _) ->
            util_lock:ets_lock(incomplete_table, NewKey, DeleteIncompleteElementCallback),
            [{nil, nil}, {nil, nil}];
        (SmallerPeer, {nil, nil}, Pid, Ref) ->
            util_lock:set_neighbor(NewKey, SmallerPeer, Level),
            Reply(IncrLevel(), Pid, Ref),
            [SmallerPeer, {nil, nil}];
        ({nil, nil}, BiggerPeer, Pid, Ref) ->
            util_lock:set_neighbor(NewKey, BiggerPeer, Level),
            Reply(IncrLevel(), Pid, Ref),
            [BiggerPeer, {nil, nil}];
        (SmallerPeer, BiggerPeer, Pid, Ref) ->
            util_lock:set_neighbor(NewKey, SmallerPeer, Level),
            util_lock:set_neighbor(NewKey, BiggerPeer, Level),
            Reply(IncrLevel(), Pid, Ref),
            [SmallerPeer, BiggerPeer]
    end,

    io:format("join ~p~n", [Level]),
    case gen_server:call(InitServer, {InitKey, {'join-process-0', {whereis(?MODULE), NewKey, MVector}}, Level}, ?TIMEOUT) of
        {exist, {_Server, _Key}} ->
            io:format("~n~nexist~n~n~n"),
            pass;
        {ok, {SmallerPeer, BiggerPeer}, {Pid, Ref}} ->
            [NextPeer, NextOtherPeer] = JoinProcess(SmallerPeer, BiggerPeer, Pid, Ref),
            io:format("~n~nNextPeer=~p~n~n=n", [NextPeer]),
            join(NextPeer, NewKey, MVector, Level + 1, NextOtherPeer);
        {error, mismatch} ->
            io:format("~n~nerror mismatch~n~n~n"),
            case OtherPeer of
                {nil, nil} -> util_lock:ets_lock(incomplete_table, NewKey, DeleteIncompleteElementCallback);
                _          -> join(OtherPeer, NewKey, MVector, Level, {nil, nil})
            end;
        Message ->
            io:format("*ERR* join/5:unknown message: ~p~n", [Message]),
            error
    end.


remove(Key) ->
    case ets:lookup(peer_table, Key) of
        [] -> {error, noexist};
        [{Key, _}] ->
            F = fun(Item) ->
                    case Item of
                        [] ->
                            {ok, {Key, {{join, ?LEVEL_MAX}, {remove, ?LEVEL_MAX}}}};
                        [{Key, {{join, Max}, {remove, _}}}] ->
                            case Max of
                                -1 -> {delete, Key};
                                _  -> {ok, {Key, {{join, Max}, {remove, Max}}}}
                            end
                    end
            end,
            util_lock:ets_lock(incomplete_table, Key, F),
            remove(Key, ?LEVEL_MAX - 1)
    end.

remove(Key, -1) ->
    ets:delete(peer_table, Key);
remove(Key, Level) ->
    Result = gen_server:call(?MODULE, {Key, {'remove-process-1', {Key}}, Level}),
    case Result of
        {already} ->
            {error, 'already-removing'};
        {ok, removed} ->
            util_lock:set_neighbor(Key, {nil, nil}, Level),
            F = fun([{_Key, {{join, _N}, {remove, _Level}}}]) ->
                    {ok, {Key, {{join, _N}, {remove, Level - 1}}}}
            end,
            util_lock:ets_lock(incomplete_table, Key, F),
            remove(Key, Level - 1)
    end.


%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(Arg) ->
    spawn(?MODULE, init_tables, [Ref=make_ref(), self()]),
    receive {ok, Ref} -> ok end,
    util_lock:lock_init([lockdaemon_join_table, lockdaemon_ets_table]),
    util_lock:wakeup_init(),
    {ok, Arg}.

init_tables(Ref, Return) ->
    case catch ets:lookup(node_ets_table, ?MODULE) of
        {'EXIT', {badarg, _}} ->
            ets:new(peer_table, [ordered_set, public, named_table]),
            ets:new(attribute_table, [set, public, named_table]),
            ets:new(incomplete_table, [set, public, named_table]),
            ets:new(node_ets_table, [set, public, named_table]);
        _ -> ok
    end,
    Return ! {ok, Ref},
    receive after infinity -> loop end.


%%--------------------------------------------------------------------
%% Function: handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                                {reply, Reply, State, Timeout} |
%%                                                {noreply, State} |
%%                                                {noreply, State, Timeout} |
%%                                                {stop, Reason, Reply, State} |
%%                                                {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({peer, random}, _From, State) ->
    [{SelfKey, _} | _] = ets:tab2list(peer_table),
    io:format("~n~n~n~n~nmatch ok~n~n~n~n~n"),
    {reply, {self(), SelfKey}, State};

handle_call({'get-ets-table'}, _From, State) ->
    io:format("get-ets-table~n"),
    [{?MODULE, Tab}] = ets:lookup(node_ets_table, ?MODULE),
    {reply, Tab, State};

%% ピア(SelfKey)のValueを書き換える．join処理と組み合わせて使用．
handle_call({_Key, {put, _ParentKey}}, _From, State) ->
    %%
    %% TODO
    %%
    {reply, ok, State};

%% lookupメッセージを受信し、適当なローカルピア(無ければグローバルピア)を選択して，lookup_process_0に繋げる．
handle_call({lookup, Key0, Key1, AttributeList, From}, _From, [InitServer | Tail]) ->
    spawn_link(sg_lookup,lookup, [InitServer, Key0, Key1, AttributeList, {Ref=make_ref(), From}]),
    {reply, Ref, [InitServer | Tail]};

%% 最適なピアを探索する．
handle_call({SelfKey, {'lookup-process-0', {Key0, Key1, AttributeList, From}}}, _From, State) ->
    spawn_link(sg_lookup, process_0, [SelfKey, Key0, Key1, AttributeList, From]),
    {reply, nil, State};

%% 指定された範囲内を走査し，値を収集する．
handle_call({SelfKey, {'lookup-process-1', {Key0, Key1, AttributeList, From}}}, _From, State) ->
    spawn_link(sg_lookup, process_1, [SelfKey, Key0, Key1, AttributeList, From]),
    {noreply, State};

%% joinメッセージを受信し、適当なローカルピア(無ければグローバルピア)を返す．
handle_call({'select-first-peer', _NewKey}, From, [InitServer | Tail]) ->
    case ets:tab2list(peer_table) of
        [{SelfKey, _} | _]-> gen_server:reply(From, {ok, {self(), SelfKey}});
        [] ->
            case InitServer of
                nil -> gen_server:reply(From, {ok, {nil, nil}});
                _ ->
                    {_, Key} = gen_server:call(InitServer, {peer, random}),
                    gen_server:reply(From, {ok, {InitServer, Key}})
            end
    end,
    {noreply, [InitServer | Tail]};

%% join-process-0にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'join-process-0', {Server, NewKey, MVector}}, Level}, From, State) ->
    spawn_link(gen_server, call, [self(), {SelfKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level}]),
    {noreply, State};

%% sg_join:process_0/3関数をutil_lock:join_lockに与える
handle_call({SelfKey, {'join-process-0', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() -> sg_join:process_0(SelfKey, From, Server, NewKey, MVector, Level) end,
    spawn_link(util_lock, join_lock, [{SelfKey, Level}, F]),
    {reply, nil, State};

%% ネットワーク先ノードから呼び出されるために存在する．
%% sg_join:process_1を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'join-process-1', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() -> sg_join:process_1(SelfKey, From, Server, NewKey, MVector, Level) end,
    %util_lock:join_lock({SelfKey, Level}, F),
    spawn_link(util_lock, join_lock, [{SelfKey, Level}, F]),
    {reply, nil, State};

%% join-process-0-onewayにFromを渡して再度呼び出す．
handle_call({SelfKey, {'join-process-0-oneway', {Server, NewKey, MVector}}, Level}, From, State) ->
    spawn_link(gen_server, call, [self(), {SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MVector}}, Level}]),
    {noreply, State};

%% sg_join:process_0_oneway/3関数をutil_lock:join_lockに与える
handle_call({SelfKey, {'join-process-0-oneway', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() -> sg_join:process_0_oneway(SelfKey, From, Server, NewKey, MVector, Level) end,
    spawn_link(util_lock, join_lock, [{SelfKey, Level}, F]),
    %util_lock:join_lock({SelfKey, Level}, F),
    {noreply, State};

%% sg_join:process_1_oneway/3関数を呼び出す．ローカルからこのコールバック関数を呼び出すことはほとんどない．
handle_call({SelfKey, {'join-process-1-oneway', {From, Server, NewKey, MVector}}, Level}, _From, State) ->
    F = fun() -> sg_join:process_1_oneway(SelfKey, From, Server, NewKey, MVector, Level) end,
    %util_lock:join_lock({SelfKey, Level}, F),
    spawn_link(util_lock, join_lock, [{SelfKey, Level}, F]),
    {reply, nil, State};

%% ローカルで呼び出すことはない．Neighborをupdateする．
handle_call({SelfKey, {'join-process-2', {From, Server, NewKey}}, Level, Other}, _From, State) ->
    Self = self(),
    F = fun() ->
            Ref = make_ref(),
            util_lock:set_neighbor(SelfKey, {Server, NewKey}, Level),
            if
                NewKey < SelfKey -> gen_server:reply(From, {ok, {Other, {Self, SelfKey}}, {self(), Ref}});
                SelfKey < NewKey -> gen_server:reply(From, {ok, {{Self, SelfKey}, Other}, {self(), Ref}})
            end,
            % reply先がNeighborの更新に成功するまで待機
            receive
                {ok, Ref} ->
                    case ets:lookup(node_ets_table, Server) of
                        [] -> spawn_link(fun() -> ets:insert(node_ets_table, {Server, gen_server:call(Server, {'get-ets-table'})}) end);
                        _  -> ok
                    end
            end
    end,
    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};

%% remove-process-0にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'remove-process-0', {RemovedKey}}, Level}, From, State) ->
    spawn_link(gen_server, call, [self(), {SelfKey, {'remove-process-0', {From, RemovedKey}}, Level}]),
    {noreply, State};

%% sg_remove:process_0/3関数をutil_lock:join_lockに与える
handle_call({SelfKey, {'remove-process-0', {From, RemovedKey}}, Level}, _From, State) ->
    F = fun() -> sg_remove:process_0(SelfKey, {From, RemovedKey}, Level) end,
    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};

%% remove-process-1にFromを渡して再度呼ぶ．
handle_call({SelfKey, {'remove-process-1', {RemovedKey}}, Level}, From, State) ->
    spawn_link(gen_server, call, [self(), {SelfKey, {'remove-process-1', {From, RemovedKey}}, Level}]),
    {noreply, State};

%% ネットワーク先ノードから呼び出されるために存在する．
%% sg_remove:process_1を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-1', {From, RemovedKey}}, Level}, _From, State) ->
    F = fun() -> sg_remove:process_1(SelfKey, {From, RemovedKey}, Level) end,
    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};

%% ネットワーク先ノードから呼び出されるために存在する．
%% sg_remove:process_2を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-2', {From, RemovedKey}}, NewNeighbor, Level}, _From, State) ->
    F = fun() -> sg_remove:process_2(SelfKey, {From, RemovedKey}, NewNeighbor, Level) end,
    util_lock:join_lock({SelfKey, Level}, F),
    {reply, nil, State};

%% ネットワーク先ノードから呼び出されるために存在する．
%% sg_remove:process_3を，他の処理をlockして呼び出す．
handle_call({SelfKey, {'remove-process-3', {From, RemovedKey}}, NewNeighbor, Level}, _From, State) ->
    sg_remove:process_3(SelfKey, {From, RemovedKey}, NewNeighbor, Level),
    {reply, nil, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(_Message, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_, _) ->
    ok.


%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _NewVsn) ->
    {ok, State}.
