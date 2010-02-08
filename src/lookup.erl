-module(lookup).
-export([lookup/5, process_0/5, process_1/6]).

lookup(InitialNode, Key0, Key1, TypeList, From) ->
    PeerList = ets:tab2list('Peer'),
    case PeerList of
        [] ->
            case InitialNode of
                '__none__' ->
                    gen_server:reply(From,
                        {ok, {'__none__', '__none__'}});

                _ ->
                    {_, InitKey} = gen_server:call(InitialNode, {peer, random}),
                    gen_server:call(InitialNode,
                        {InitKey,
                            {'lookup-process-0',
                                {Key0, Key1, TypeList, From}}})
            end;

        _ ->
            [{SelfKey, {_, _, _}} | _] = PeerList,
            gen_server:call(?MODULE,
                {SelfKey,
                    {'lookup-process-0',
                        {Key0, Key1, TypeList, From}}})
    end.

process_0(SelfKey, Key0, Key1, TypeList, From) ->
    [{SelfKey, {_, _, {Smaller, Bigger}}}] = ets:lookup('Peer', SelfKey),

    {Neighbor, S_or_B} = if
        Key0 =< SelfKey -> {Smaller, smaller};
        SelfKey < Key0 -> {Bigger, bigger}
    end,

    case util:select_best(Neighbor, Key0, S_or_B) of
        % 最適なピアが見つかったので、次のフェーズ(lookup-process-1)へ移行
        {'__none__', '__none__'} ->
            gen_server:call(?MODULE,
                {SelfKey,
                    {'lookup-process-1',
                        {Key0, Key1, TypeList, From},
                        []}});
        {'__self__'} ->
            gen_server:call(?MODULE,
                {SelfKey,
                    {'lookup-process-1',
                        {Key0, Key1, TypeList, From},
                        []}});

        % 探索するキーが一つで，かつそれを保持するピアが存在した場合，ETSテーブルに直接アクセスする
        {BestNode, Key0} when Key0 == Key1 ->
            case ets:lookup('ETS-Table', BestNode) of
                [{BestNode, Tab}] ->
                    [{BestNode, Tab} | _] = ets:lookup('ETS-Table', BestNode),
                    case ets:lookup(Tab, Key0) of
                        [{Key0, {Value, _, _}} | _] ->
                            io:format("ets~n"),
                            gen_server:reply(From, {ok, [{Key0, Value}]});

                        [] ->
                            gen_server:reply(From, {ok, []})
                    end;

                _ ->
                    spawn(fun() ->
                                gen_server:call(BestNode,
                                    {Key0,
                                        {'lookup-process-0',
                                            {Key0, Key1, TypeList, From}}})
                        end)
            end;

        % 最適なピアの探索を続ける(lookup-process-0)
        {BestNode, BestKey} ->
            spawn(fun() ->
                        gen_server:call(BestNode,
                            {BestKey,
                                {'lookup-process-0',
                                    {Key0, Key1, TypeList, From}}})
                end)
    end.

process_1(SelfKey, Key0, Key1, TypeList, From, ItemList) ->
    if
        SelfKey < Key0 ->
            [{SelfKey, {_, _, {_, [{NextNode, NextKey} | _]}}}] = ets:lookup('Peer', SelfKey),
            case {NextNode, NextKey} of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {ok, ItemList});
                _ ->
                    gen_server:call(NextNode,
                        {NextKey,
                            {'lookup-process-1',
                                {Key0, Key1, TypeList, From},
                                ItemList}})
            end;

        Key1 < SelfKey ->
            gen_server:reply(From, {ok, ItemList});

        true ->
            [{SelfKey, {Value, _, {_, [{NextNode, NextKey} | _]}}}] = ets:lookup('Peer', SelfKey),
            case {NextNode, NextKey} of
                {'__none__', '__none__'} ->
                    gen_server:reply(From, {ok, [{SelfKey, Value} | ItemList]});
                _ ->
                    gen_server:call(NextNode,
                        {NextKey,
                            {'lookup-process-1',
                                {Key0, Key1, TypeList, From},
                                [{SelfKey, Value} | ItemList]}})
            end
    end.

