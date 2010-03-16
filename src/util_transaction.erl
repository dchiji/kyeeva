
-module(ets_lock).
-export([]).


%%====================================================================
%% Processes Transactions
%%====================================================================

%% {trap}を受信したら，PListに含まれる全プロセスに{ok, Ref}メッセージを送信する．
wait_trap(PList) ->
    receive
        {add, {Pid, Ref}} ->
            wait_trap([{Pid, Ref} | PList]);
        trap ->
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
                trap ->
                    Pid ! {ok, Ref};
                {error, Message} ->
                    Pid ! {error, Ref, Message}
            end
    end,

    wait_trapped(Receive).


%%====================================================================
%% ETS Transactions
%%====================================================================

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
    F = fun([{_, {MembershipVector, {Smaller, Bigger}}}]) ->
            io:format("NewKey=~p  SelfKey=~p~n", [NewKey, SelfKey]),
            if
                {Server, NewKey} == {'__none__', '__none__'} ->
                    {SFront, [_ | STail]} = lists:split(Level, Smaller),
                    {BFront, [_ | BTail]} = lists:split(Level, Bigger),

                    NewSmaller = SFront ++ [{Server, NewKey} | STail],
                    NewBigger = BFront ++ [{Server, NewKey} | BTail],

                    %io:format("update: SelfKey=~p, NewKey=~p, Level=~p  ~p~n", [SelfKey, NewKey, Level, smaller]),
                    {ok, {SelfKey, {MembershipVector, {NewSmaller, NewBigger}}}};

                NewKey < SelfKey ->
                    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Smaller),

                    NewSmaller = Front ++ [{Server, NewKey} | Tail],
                    %io:format("update: SelfKey=~p, NewKey=~p, Level=~p  ~p~n", [SelfKey, NewKey, Level, smaller]),
                    {ok, {SelfKey, {MembershipVector, {NewSmaller, Bigger}}}};

                %SelfKey =< NewKey ->
                SelfKey < NewKey ->
                    {Front, [{_OldNode, _OldKey} | Tail]} = lists:split(Level, Bigger),

                    NewBigger = Front ++ [{Server, NewKey} | Tail],
                    %io:format("update: SelfKey=~p, NewKey=~p, Level=~p  ~p~n", [SelfKey, NewKey, Level, bigger]),
                    {ok, {SelfKey, {MembershipVector, {Smaller, NewBigger}}}};

                SelfKey == NewKey ->
                    io:format("~n~n~noutput information~n~n"),
                    io:format("~p: ~p~n", [SelfKey, ets:lookup('Peer', SelfKey)]),

                    %timer:stop(infinity)
                    {ok, {SelfKey, {MembershipVector, {Smaller, Bigger}}}}
            end
    end,

    lock_update(SelfKey, F).
