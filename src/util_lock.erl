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

-module(util_lock).

-include("../include/common.hrl").

%% process transaction
-export([wakeup_init/0,
        wakeup_register/2,
        wakeup/1,
        wakeup_with_report/2]).

%% ETS transaction
-export([lock_init/1,
        lock_daemon/0,
        lock_daemon_callback_ets_lock/3,
        join_lock/2,
        ets_lock/3,
        lock/4,
        set_neighbor/3]).

-define(NUM_OF_LOCK_PROCESS, 50).
-define(JOIN_LOCK_TABLE, lockdaemon_join_table).
-define(ETS_LOCK_TABLE, lockdaemon_ets_table).


%%====================================================================
%% process Transactions
%%====================================================================

wakeup_init() ->
    ets:new(sleeping_process_name_table, [bag, public, named_table]).


wakeup_register(Key, Pid) ->
    ets:insert(sleeping_process_table, {Key, Pid}).


wakeup(Key) ->
    wakeup_common(Key, getup).

wakeup_with_report(Key, Reason) ->
    wakeup_common(Key, {error, Reason}).

wakeup_common(Key, Message) ->
    PidList = ets:lookup(sleeping_process_name_table, Key),
    ets:delete(sleeping_process_name_table, Key),
    lists:foreach(fun(Pid) -> Pid ! Message end, PidList).


%%====================================================================
%% ETS Transactions
%%====================================================================

lock_init(TableNameList) ->
    Init = fun(Name) ->
            ets:new(Name, [set, public, named_table]),
            [ets:insert(Name, {HashN, spawn(fun lock_daemon/0)}) || HashN <- lists:seq(0, ?NUM_OF_LOCK_PROCESS - 1)]
    end,
    [Init(Name) || Name <- TableNameList].


%% a daemon process for each hashes
lock_daemon() ->
    receive
        {{From, Ref}, Callback, Args} ->
            From ! {Ref, erlang:apply(Callback, Args)}
    end,
    lock_daemon().

lock_daemon_callback_ets_lock(Table, Key, F) ->
    case ets:lookup(Table, Key) of
        []     -> pass;    %% TODO
        [Item] -> lock_daemon_callback_ets_lock_1(Table, F([Item]))
    end.

lock_daemon_callback_ets_lock_1(Table, {ok, Result}) ->
    ets:insert(Table, Result),
    {ok, Result};
lock_daemon_callback_ets_lock_1(Table, {delete, DeletedKey}) ->
    ets:delete(Table, DeletedKey),
    {ok, DeletedKey};
lock_daemon_callback_ets_lock_1(_, {error, Reason}) ->
    {error, Reason};
lock_daemon_callback_ets_lock_1(_, {pass}) ->
    {ok, []}.



join_lock(Key, Callback) ->
    lock(?JOIN_LOCK_TABLE, Key, Callback, []).

ets_lock(Table, Key, F) ->
    lock(?ETS_LOCK_TABLE, Key, fun lock_daemon_callback_ets_lock/3, [Table, Key, F]).

lock(DaemonTable, Key, Callback, Args) ->
    Ref  = make_ref(),
    Hash = erlang:phash2(Key, ?NUM_OF_LOCK_PROCESS),
    case ets:lookup(DaemonTable, Hash) of
        [] ->
            %% it never match this
            ets:insert(DaemonTable, {Hash, spawn(fun lock_daemon/0)}),
            [{Hash, Daemon}] = ets:lookup(DaemonTable, Hash),
            lock_1(Daemon, Ref, Callback, Args);
        [{Hash, Daemon}] ->
            lock_1(Daemon, Ref, Callback, Args)
    end.

lock_1(Daemon, Ref, Callback, Args) ->
    Daemon ! {{self(), Ref}, Callback, Args},
    receive
        {Ref, Result} -> Result    %% receive from Daemon
    end.


set_neighbor(Key, {Server, NewKey}, Level) ->
    New = [{Key, {Server, NewKey}, Level}],
    ets_lock(peer_table, Key, fun(Old) -> set_neighbor_1(New, Old) end).

set_neighbor_1([New], []) ->
    {ok, New};
set_neighbor_1([{Key, {nil, nil}, Level}], [{Key, Pstate}]) ->
    {SFront, [_ | STail]} = lists:split(Level, Pstate#pstate.smaller),
    {BFront, [_ | BTail]} = lists:split(Level, Pstate#pstate.bigger),
    NewPstate = Pstate#pstate{
        smaller = SFront ++ [{nil, nil} | STail],
        bigger = BFront ++ [{nil, nil} | BTail]
    },
    {ok, {Key, NewPstate}};
set_neighbor_1([{Key, {Server, NewKey}, Level}], [{Key, Pstate}]) when Key > NewKey ->
    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Pstate#pstate.smaller),
    NewPstate = Pstate#pstate{
        smaller = Front ++ [{Server, NewKey} | Tail]
    },
    {ok, {Key, NewPstate}};
set_neighbor_1([{Key, {Server, NewKey}, Level}], [{Key, Pstate}]) when Key < NewKey ->
    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Pstate#pstate.bigger),
    NewPstate = Pstate#pstate{
        bigger = Front ++ [{Server, NewKey} | Tail]
    },
    {ok, {Key, NewPstate}};
set_neighbor_1([{Key, {_, NewKey}, _}], [{Key, _}=Old]) when Key == NewKey ->
    {ok, Old}.
