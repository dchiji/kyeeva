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

-module(util).

-export([wait_trap/1, wait_trapped/1]).
-export([lock_daemon/1, lock_join/2, lock_join_callback/1, lock_update/2, lock_update/3, lock_update_callback/1, update/3]).
-export([select_best/3, make_membership_vector/0]).


%-define(LEVEL_MAX, 8).
%-define(LEVEL_MAX, 16).
-define(LEVEL_MAX, 32).
%-define(LEVEL_MAX, 64).
%-define(LEVEL_MAX, 128).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).

-define(SERVER_MODULE, skipgraph).


%%====================================================================
%% Transactions
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

lock_update({Type, Value}, F) ->
    lock_update('Peer', {Type, Value}, F).

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
%% Other
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
when Key0 == Key1 ->    % rotate
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

