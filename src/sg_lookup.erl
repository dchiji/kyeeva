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

-module(sg_lookup).
-export([lookup/5, process_0/5, process_1/5, call/4]).


%-define(LEVEL_MAX, 8).
%-define(LEVEL_MAX, 16).
-define(LEVEL_MAX, 32).
%-define(LEVEL_MAX, 64).
%-define(LEVEL_MAX, 128).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).

-define(SERVER_MODULE, skipgraph).


lookup(InitialNode, Key0, Key1, TypeList, From) ->
    PeerList = ets:tab2list(peer_table),
    case PeerList of
        [] ->
            case InitialNode of
                nil ->
                    gen_server:reply(From,
                        {ok, {nil, nil}});

                _ ->
                    {_, InitKey} = gen_server:call(InitialNode, {peer, random}),
                    gen_server:call(InitialNode,
                        {InitKey,
                            {'lookup-process-0',
                                {Key0, Key1, TypeList, From}}})
            end;

        _ ->
            [{SelfKey, {_, _}} | _] = PeerList,
            gen_server:call(?SERVER_MODULE,
                {SelfKey,
                    {'lookup-process-0',
                        {Key0, Key1, TypeList, From}}})
    end.


process_0(SelfKey, Key0, Key1, TypeList, {Ref, From}) ->
    [{SelfKey, {_, {Smaller, Bigger}}}] = ets:lookup(peer_table, SelfKey),
    {Neighbor, S_or_B} = if
        Key0 =< SelfKey -> {Smaller, smaller};
        SelfKey < Key0 -> {Bigger, bigger}
    end,
    case util:select_best(Neighbor, Key0, S_or_B) of
        % 最適なピアが見つかったので、次のフェーズ(lookup-process-1)へ移行
        {nil, nil} ->
            gen_server:call(?SERVER_MODULE, {SelfKey, {'lookup-process-1', {Key0, Key1, TypeList, {Ref, From}}}});
        {'__self__'} ->
            gen_server:call(?SERVER_MODULE, {SelfKey, {'lookup-process-1', {Key0, Key1, TypeList, {Ref, From}}}});

        % 探索するキーが一つで，かつそれを保持するピアが存在した場合，ETSテーブルに直接アクセスする
        {BestNode, Key0} when Key0 == Key1 ->
            case ets:lookup(lock_ets_table, BestNode) of
                [{BestNode, Tab}] ->
                    [{BestNode, Tab} | _] = ets:lookup(lock_ets_table, BestNode),
                    case ets:lookup(Tab, Key0) of
                        [{Key0, {UniqueKey, _, _}} | _] ->
                            ValueList = case TypeList of
                                [] ->
                                    get_values(UniqueKey, [value]);
                                _ ->
                                    get_values(UniqueKey, TypeList)
                            end,
                            From ! {Ref, {'get-reply', {UniqueKey, ValueList}}},
                            From ! {Ref, {'get-end'}};
                        [] ->
                            From ! {Ref, {'get-end'}}
                    end;
                _ ->
                    gen_server:call(BestNode, {Key0, {'lookup-process-0', {Key0, Key1, TypeList, {Ref, From}}}})
            end;
        % 最適なピアの探索を続ける(lookup-process-0)
        {BestNode, BestKey} ->
            gen_server:call(BestNode, {BestKey, {'lookup-process-0', {Key0, Key1, TypeList, {Ref, From}}}})
    end.


process_1(SelfKey, Key0, Key1, TypeList, {Ref, From}) ->
    if
        SelfKey < Key0 ->
            [{SelfKey, {_, {_, [{NextNode, NextKey} | _]}}}] = ets:lookup(peer_table, SelfKey),
            case {NextNode, NextKey} of
                {nil, nil} ->
                    From ! {Ref, {'get-end'}};
                _ ->
                    gen_server:call(NextNode,
                        {NextKey,
                            {'lookup-process-1',
                                {Key0, Key1, TypeList, {Ref, From}}}})
            end;

        Key1 < SelfKey ->
            From ! {Ref, {'get-end'}};

        true ->
            [{{SelfKey, UniqueKey}, {_, {_, [{NextNode, NextKey} | _]}}}] = ets:lookup(peer_table, SelfKey),

            ValueList = case TypeList of
                [] ->
                    get_values(UniqueKey, [value]);
                _ ->
                    get_values(UniqueKey, TypeList)
            end,

            From ! {Ref, {'get-reply', {UniqueKey, ValueList}}},

            case {NextNode, NextKey} of
                {nil, nil} ->
                    From ! {Ref, {'get-end'}};
                _ ->
                    if
                        NextKey < Key0 orelse Key1 < NextKey ->
                            From ! {Ref, {'get-end'}};
                        true ->
                            gen_server:call(NextNode,
                                {NextKey,
                                    {'lookup-process-1',
                                        {Key0, Key1, TypeList, {Ref, From}}}})
                    end
            end
    end.


get_values(_UniqueKey, []) ->
    [];
get_values(UniqueKey, [Type | Tail]) ->
    case ets:lookup('Types', {UniqueKey, Type}) of
        [] ->
            get_values(UniqueKey, Tail);

        [{{UniqueKey, Type}, Value}] ->
            [{Type, Value} | get_values(UniqueKey, Tail)]
    end.


call(Module, Key0, Key1, TypeList) ->
    Ref = gen_server:call(Module, {lookup, Key0, Key1, TypeList, self()}),

    call(Ref, []).

call(Ref, List) ->
    receive
        {Ref, {'get-reply', ValueList}} ->
            call(Ref, [ValueList | List]);
        {Ref, {'get-end'}} ->
            List
    after
        1500 ->
            List
    end.

