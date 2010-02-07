
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

-module(remove).
-export([process_0/3, process_1/3, process_2/4, process_3/4]).

%-define(LEVEL_MAX, 8).
%-define(LEVEL_MAX, 16).
-define(LEVEL_MAX, 32).
%-define(LEVEL_MAX, 64).
%-define(LEVEL_MAX, 128).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).

-define(SERVER_MODULE, skipgraph).


%% handle_call<remove-process-0>から呼び出される．
%% RemovedKeyピアを探索．
%% ただ，ほとんど使用しない．
process_0(SelfKey, {From, RemovedKey}, Level) ->
    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {IsSelf, {NeighborNode, NeighborKey}, S_or_B} = if
        SelfKey == RemovedKey ->
            {true, lists:nth(Level + 1, Smaller), smaller};
        SelfKey < RemovedKey ->
            {false, lists:nth(Level + 1, Bigger), bigger};
        RemovedKey < SelfKey ->
            {false, lists:nth(Level + 1, Smaller), smaller}
    end,

    case IsSelf of
        true ->
            spawn(fun() ->
                        remove(RemovedKey)
                end);

        false ->
            spawn(fun() ->
                        gen_server:call(NeighborNode,
                            {NeighborKey,
                                {'remove-process-0',
                                    {From,
                                        RemovedKey},
                                    Level}})
                end)
    end.


%% handle_call<remove-process-1>から呼び出される．
%% このとき，SelfKey == RemovedKeyはtrue．
%% 片方のNeighborにremove-process-2メッセージを送信．
process_1(SelfKey, {From, RemovedKey}, Level) ->
    io:format("Peer[SelfKey] = ~p~n", [ets:lookup('Peer', SelfKey)]),

    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    case lists:nth(Level + 1, Smaller) of
        {'__none__', '__none__'} ->
            case lists:nth(Level + 1, Bigger) of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {ok, removed});

                {BiggerNode, BiggerKey} ->
                    spawn(fun() ->
                                gen_server:call(BiggerNode,
                                    {BiggerKey,
                                        {'remove-process-2',
                                            {From,
                                                RemovedKey}},
                                        Smaller,
                                        Level},
                                    ?TIMEOUT)
                        end)
            end;

        {SmallerNode, SmallerKey} ->
            spawn(fun() ->
                        gen_server:call(SmallerNode,
                            {SmallerKey,
                                {'remove-process-2',
                                    {From,
                                        RemovedKey}},
                                Bigger,
                                Level},
                            ?TIMEOUT)
                end)
    end.


%% handle_call<remove-process-2>から呼び出される．
%% 実際にremove処理を行い，NewNeighborにremove-process-3メッセージを送信．
process_2(SelfKey, {From, RemovedKey}, NewNeighbor, Level) ->
    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {OldNode, OldKey} = if
        SelfKey < RemovedKey ->
            lists:nth(Level + 1, Smaller);
        RemovedKey < SelfKey ->
            lists:nth(Level + 1, Bigger)
    end,

    case OldKey of
        RemovedKey ->
            case NewNeighbor of
                {'__none__', '__none__'} ->
                    update(SelfKey, NewNeighbor, Level);

                {NewNode, NewKey} ->
                    gen_server:call(NewNode,
                        {NewKey,
                            {'remove-process-3',
                                {From,
                                    RemovedKey},
                                {whereis(?SERVER_MODULE), SelfKey},
                                Level}}),

                    update(SelfKey, NewNeighbor, Level)
            end;

        _ ->
            pass
    end,

    gen_server:reply(From, {ok, removed}).

process_3(SelfKey, {From, RemovedKey}, NewNeighbor, Level) ->
    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {OldNode, OldKey} = if
        SelfKey < RemovedKey ->
            lists:nth(Level + 1, Smaller);
        RemovedKey < SelfKey ->
            lists:nth(Level + 1, Bigger)
    end,

    case OldKey of
        RemovedKey ->
            update(SelfKey, NewNeighbor, Level);
        _ ->
            pass
    end.

