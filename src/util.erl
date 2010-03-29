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

-export([select_best/3]).

%-define(TIMEOUT, 3000).
-define(TIMEOUT, infinity).
-define(SERVER_MODULE, skipgraph).


%% Neighborの中から最適なピアを選択する
select_best([], _, _) ->
    {nil, nil};
select_best([{nil, nil} | _], _, _) ->
    {nil, nil};
select_best([{Node0, Key0} | _], Key, _) when Key0 == Key ->
    {Node0, Key0};
select_best([{Node0, Key0}], Key, S_or_B) when S_or_B == smaller ->
    if
        Key0 < Key ->
            {self, self};
        true ->
            {Node0, Key0}
    end;
select_best([{Node0, Key0}], Key, S_or_B) when S_or_B == bigger ->
    if
        Key < Key0 ->
            {self, self};
        true ->
            {Node0, Key0}
    end;
select_best([{Node0, Key0}, {nil, nil} | _], Key, S_or_B) when S_or_B == smaller ->
    if
        Key0 < Key ->
            {self, self};
        true ->
            {Node0, Key0}
    end;
select_best([{Node0, Key0}, {nil, nil} | _], Key, S_or_B) when S_or_B == bigger ->
    if
        Key < Key0 ->
            {self, self};
        true ->
            {Node0, Key0}
    end;
select_best([{_Node0, Key0}, {Node1, Key1} | Tail], Key, S_or_B) when Key0 == Key1 ->    % rotate
    select_best([{Node1, Key1} | Tail], Key, S_or_B);
select_best([{Node0, Key0}, {Node1, Key1} | Tail], Key, S_or_B) when S_or_B == smaller ->
    if
        Key0 < Key ->
            {self, self};
        Key1 < Key ->
            {Node0, Key0};
        Key < Key0 ->
            select_best([{Node1, Key1} | Tail], Key, S_or_B)
    end;
select_best([{Node0, Key0}, {Node1, Key1} | Tail], Key, S_or_B) when S_or_B == bigger ->
    if
        Key < Key0 ->
            {self, self};
        Key < Key1 ->
            {Node0, Key0};
        true ->
            select_best([{Node1, Key1} | Tail], Key, S_or_B)
    end.
