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

-module(util_transaction).

%% process transaction
-export([wait_trap/1]).

%% ETS transaction
-export([lock_daemon/0,
        lock_update_callback/3,
        join_lock/2,
        ets_lock/3,
        lock/4,
        set_neighbor/3]).

-define(NUM_OF_LOCK_process, 10).
-define(JOIN_LOCK_TABLE, 'Lock-Join-Daemon').
-define(ETS_LOCK_TABLE, 'Lock-Update-Daemon').


%%====================================================================
%% process Transactions
%%====================================================================

%% when I receive "trap" message, I will send "{ok, Ref}" message to all process I store
wait_trap(PList) ->
    receive
        {add, {Pid, Ref}} ->
            wait_trap([{Pid, Ref} | PList]);
        {error, Message} ->
            lists:foreach(fun({Pid, Ref}) -> Pid ! {error, Ref, Message} end,PList),
            wait_trap_1({error, Message});
        trap ->
            lists:foreach(fun({Pid, Ref}) -> Pid ! {ok, Ref} end, PList),
            wait_trap_1(trap)
    end.

wait_trap_1(Received) ->
    receive
        {add, {Pid, Ref}} ->
            wait_trapped_2(Received)
    end,
    wait_trap_1(Receive).

wait_trap_2(Received) ->
    case Received of
        trap ->
            Pid ! {ok, Ref};
        {error, Message} ->
            Pid ! {error, Ref, Message}
    end.


%%====================================================================
%% ETS Transactions
%%====================================================================

%% a daemon process for each hashes
lock_daemon() ->
    receive
        {{From, Ref}, Callback, Args} ->
            From ! {Ref, erlang:apply(Callback, Arg)}
    end,
    lock_daemon().

lock_update_callback(Table, Key, F) ->
    [Item] = ets:lookup(Table, Key),
    lock_update_callback_1(F(Item)).

lock_update_callback_1({ok, Result}) ->
    ets:insert(Table, Result),
    {ok, Result};
lock_update_callback_1({delete, DeletedKey}) ->
    ets:delete(Table, DeletedKey),
    {ok, DeletedKey};
lock_update_callback_1({error, Reason}) ->
    {error, Reason};
lcok_update_callback_1({pass}) ->
    {ok, []}.


join_lock(Key, Callback) ->
    lock(?JOIN_LOCK_TABLE, Key, Callback, []).

ets_lock(Table, Key, F) ->
    lock(?ETS_LOCK_TABLE, Key, fun lock_update_callback/3, [Table, Key, F]).

lock(DaemonTable, Key, Callback, Args) ->
    Ref  = make_ref(),
    Hash = phash2(Key, ?NUM_OF_LOCK_process),

    case ets:lookup(DaemonTable, Hash) of
        [] ->
            %% it never match this
            ets:insert(DaemonTable, {Hash, spawn(fun lock_daemon/0)}),
            [{Hash, Daemon}] = ets:lookup(DaemonTable, Hash),
            Daemon ! {{self(), Ref}, Callback, Args};
        [{Hash, Daemon}] ->
            Daemon ! {{self(), Ref}, Callback, Args}
    end,

    receive
        {Ref, ok} ->
            %% receive from Daemon
            ok
    end.


set_neighbor(Key, {Server, NewKey}, Level) ->
    New = [{Key, {Server, NewKey}, Level}]
    ets_lock(peer_table, Key, fun(Old) -> set_neighbor_1(New, Old) end).

set_neighbor_1([New], []) ->
    {ok, New};
set_neighbor_1([{Key, {nil, nil}, Level}], [{Key, {MembershipVector, {Smaller, Bigger}}}]) ->
    {SFront, [_ | STail]} = lists:split(Level, Smaller),
    {BFront, [_ | BTail]} = lists:split(Level, Bigger),
    NewSmaller = SFront ++ [{nil, nil} | STail],
    NewBigger  = BFront ++ [{nil, nil} | BTail],
    {ok, {Key, {MembershipVector, {NewSmaller, NewBigger}}}};
set_neighbor_1([{Key, {Server, NewKey}, Level}], [{Key, {MembershipVector, {Smaller, Bigger}}}]) when NewKey < SelfKey ->
    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Smaller),
    NewSmaller = Front ++ [{Server, NewKey} | Tail],
    {ok, {Key, {MembershipVector, {NewSmaller, Bigger}}}};
set_neighbor_1([{Key, {Server, NewKey}, Level}], [{Key, {MembershipVector, {Smaller, Bigger}}}]) when NewKey > SelfKey ->
    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Bigger),
    NewBigger = Front ++ [{Server, NewKey} | Tail],
    {ok, {Key, {MembershipVector, {Smaller, NewBigger}}}};
set_neighbor_1([{Key, {Server, NewKey}, Level}], [{Key, {MembershipVector, {Smaller, Bigger}}}]) when NewKey == SelfKey ->
    {ok, {Key, {MembershipVector, {Smaller, Bigger}}}}.
