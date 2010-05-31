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

-include("../include/common_sg.hrl").


lookup(InitialNode, Key0, Key1, TypeList, From) ->
    PeerList = ets:tab2list(peer_table),
    case PeerList of
        [] ->
            case InitialNode of
                nil -> gen_server:reply(From, {ok, {nil, nil}});
                _ ->
                    {_, InitKey} = gen_server:call(InitialNode, {peer, random}),
                    gen_server:call(InitialNode, {InitKey, {'lookup-process-0', {Key0, Key1, TypeList, From}}})
            end;
        _ ->
            [{SelfKey, _} | _] = PeerList,
            gen_server:call(?SERVER_MODULE, {SelfKey, {'lookup-process-0', {Key0, Key1, TypeList, From}}})
    end.


process_0({SelfKey, _}=Key, Key0, Key1, TypeList, {Ref, From}) ->
    [{_, Peer}] = ets:lookup(peer_table, Key),
    Smaller = Peer#pstate.smaller,
    Bigger = Peer#pstate.bigger,
    {Neighbor, S_or_B} = if
        Key0 == SelfKey -> {nil, self};
        Key0 < SelfKey -> {Smaller, smaller};
        SelfKey < Key0 -> {Bigger, bigger}
    end,
    case util:select_best(Neighbor, Key0, S_or_B) of
        % 最適なピアが見つかったので、次のフェーズ(lookup-process-1)へ移行
        {nil, nil} -> process_1(Key, Key0, Key1, TypeList, {Ref, From});
        {self, self} -> process_1(Key, Key0, Key1, TypeList, {Ref, From});

        % 探索するキーが一つで，かつそれを保持するピアが存在した場合，ETSテーブルに直接アクセスする
        {BestNode, BestKey} when Key0 == Key1 ->
            case ets:lookup(node_ets_table, BestNode) of
                [{BestNode, Tab}] ->
                    case ets:lookup(Tab, BestKey) of
                        [] -> From ! {Ref, {'lookup-end'}};
                        [{_, {ParentKey, _, _}} | _] ->
                            ValueList = case TypeList of
                                [] -> get_values(ParentKey, [value]);
                                _  -> get_values(ParentKey, TypeList)
                            end,
                            From ! {Ref, {'lookup-reply', {ParentKey, ValueList}}},
                            From ! {Ref, {'lookup-end'}}
                    end;
                _ -> gen_server:call(BestNode, {BestKey, {'lookup-process-0', {Key0, Key1, TypeList, {Ref, From}}}})
            end;
        % 最適なピアの探索を続ける(lookup-process-0)
        {BestNode, BestKey} ->
            gen_server:call(BestNode, {BestKey, {'lookup-process-0', {Key0, Key1, TypeList, {Ref, From}}}})
    end.


process_1({SelfKey, _}=Key, Key0, Key1, TypeList, {Ref, From}) ->
    if
        SelfKey < Key0 ->
            [{_, Peer}] = ets:lookup(peer_table, Key),
            {NextNode, NextKey} = lists:nth(1, Peer#pstate.bigger),
            case {NextNode, NextKey} of
                {nil, nil} -> From ! {Ref, {'lookup-end'}};
                _ -> gen_server:call(NextNode, {NextKey, {'lookup-process-1', {Key0, Key1, TypeList, {Ref, From}}}})
            end;
        Key1 < SelfKey ->
            From ! {Ref, {'lookup-end'}};
        true ->
            [{_, Peer}] = ets:lookup(peer_table, Key),
            ParentKey = Peer#pstate.parent_key,
            {NextNode, NextKey} = lists:nth(1, Peer#pstate.bigger),
            ValueList = case TypeList of
                [] -> get_values(ParentKey, [value]);
                _  -> get_values(ParentKey, TypeList)
            end,
            From ! {Ref, {'lookup-reply', {ParentKey, ValueList}}},
            case {NextNode, NextKey} of
                {nil, nil} -> From ! {Ref, {'lookup-end'}};
                {NextNode, {KeyN, _}} ->
                    if
                        KeyN < Key0 orelse Key1 < KeyN -> From ! {Ref, {'lookup-end'}};
                        true -> gen_server:call(NextNode, {NextKey, {'lookup-process-1', {Key0, Key1, TypeList, {Ref, From}}}})
                    end
            end
    end.


get_values(_ParentKey, []) ->
    [];
get_values(ParentKey, [Type | Tail]) ->
    case ets:lookup(attribute_table, {ParentKey, Type}) of
        [] -> get_values(ParentKey, Tail);
        [{{ParentKey, Type}, Value}] -> [{Type, Value} | get_values(ParentKey, Tail)]
    end.


call(Module, Key0, Key1, TypeList) ->
    call(gen_server:call(Module, {lookup, Key0, Key1, TypeList, self()}), []).

call(Ref, List) ->
    receive
        {Ref, {'lookup-reply', ValueList}} -> call(Ref, [ValueList | List]);
        {Ref, {'lookup-end'}} -> List
    after 1500 -> List
    end.

