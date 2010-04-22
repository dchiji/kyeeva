
%% TODO successor-list の更新処理の実装

-module(chord_server).
-behaviour(gen_server).

-include("../include/common_chord.hrl").

%% API
-export([start/0,
        start/1,
        get/1,
        put/2,
        call/4,
        server/0]).

%% spawned functions
-export([lookup_op/4]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).


%%====================================================================
%% API
%%====================================================================
start() ->
    start(nil).

start(InitNode) ->
    spawn(fun() ->
                ets:new(store, [named_table, set, public]),
                timer:sleep(infinity)
        end),
    gen_server:start({local, ?MODULE}, ?MODULE, [InitNode], []).


get(Key) ->
    {_, Pair={Key, _}} = gen_server:call(?MODULE, {lookup_op, Key}),
    [Pair].


put(Key, Value) ->
    put(?MODULE, Key, Value).

put(Server, Key, Value) ->
    {SelectedServer, _} = gen_server:call(Server, {lookup_op, Key}),
    case gen_server:call(SelectedServer, {put_op, Key, Value}) of
        {error, not_me} -> put(SelectedServer, Key, Value);
        ok              -> ok
    end.


call(Key, ModuleName, FuncName, Args) ->
    call(?MODULE, Key, ModuleName, FuncName, Args).

call(Server, Key, ModuleName, FuncName, Args) ->
    {SelectedServer, _} = gen_server:call(Server, {lookup_op, Key}),
    case gen_server:call(SelectedServer, {call, Key, ModuleName, FuncName, Args}) of
        {error, not_me} -> call(SelectedServer, Key, ModuleName, FuncName, Args);
        Any             -> Any
    end.


server() ->
    {ok, whereis(?MODULE)}.


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
init([nil]) ->
    State = #state {
        myhash = myhash(),
        succlist = #succlist {
            bigger_len = 0,
            bigger = [],
            smaller = []
        }
    },
    {ok, State};
init([InitNode]) ->
    case net_adm:ping(InitNode) of
        pong -> {ok, init_successor_list(InitNode)};
        pang -> {error, {not_found, InitNode}}
    end.

init_successor_list(InitNode) ->
    MyHash = myhash(),
    State = #state {
        myhash = MyHash
    },
    case rpc:call(InitNode, ?MODULE, server, []) of
        {ok, InitServer} -> State#state{succlist=gen_server:call(InitServer, {join_op, MyHash})};
        {badrpc, Reason} -> {stop, Reason}
    end.


%%--------------------------------------------------------------------
%% Function: handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                                {reply, Reply, State, Timeout} |
%%                                                {noreply, State} |
%%                                                {noreply, State, Timeout} |
%%                                                {stop, Reason, Reply, State} |
%%                                                {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({join_op, NewHash}, From, State) ->
    {noreply, State#state{succlist=join_op(NewHash, From, State#state.myhash, State#state.succlist)}};

handle_call({lookup_op, Key}, From, State) ->
    spawn(?MODULE, lookup_op, [Key, State#state.myhash, From, State#state.succlist]),
    {noreply, State};

handle_call({put_op, Key, Value}, _, State) ->
    {reply, put_op(Key, Value, State#state.myhash, State#state.succlist), State};

handle_call({call, Key, Module, Func, Args}, _, State) ->
    {reply, call_op(Key, Module, Func, Args, State#state.myhash, State#state.succlist), State};

handle_call(_, _, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({join_op_cast, NewHash, From}, State) ->
    {noreply, State#state{succlist=join_op(NewHash, From, State#state.myhash, State#state.succlist)}};

handle_cast({lookup_op_cast, Key, From}, State) ->
    spawn(?MODULE, lookup_op, [Key, State#state.myhash, From, State#state.succlist]),
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_, State) ->
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
code_change(_, State, _) ->
    {ok, State}.


%%====================================================================
%% called by handle_call/3
%%====================================================================
%%--------------------------------------------------------------------
%% join operation
%%--------------------------------------------------------------------
join_op(NewHash, From, MyHash, MySuccList) ->
    {S_or_B, SuccList, Len} = select_succlist(NewHash, MyHash, MySuccList),
    case next(S_or_B, NewHash, SuccList, Len) of
        biggest ->
            join_op_biggest(S_or_B, NewHash, From, MyHash, MySuccList);
        self ->
            join_op_1(S_or_B, NewHash, From, MySuccList, MySuccList);
        {shortage, self} ->
            join_op_shortage(S_or_B, NewHash, From, MyHash, MySuccList);
        {shortage, {OtherServer, _}} ->
            gen_server:cast(OtherServer, {join_op_cast, NewHash, From}),
            MySuccList;
        {error, Reason} ->
            gen_server:reply(From, {error, Reason}),
            MySuccList;
        {OtherServer, _} ->
            gen_server:cast(OtherServer, {join_op_cast, NewHash, From}),
            MySuccList
    end.

%% the first argument must be 'smaller'
join_op_biggest(smaller, NewHash, From, MyHash, SuccList) ->
    case {SuccList#succlist.bigger_len, SuccList#succlist.bigger} of
        {0, _}  -> join_op_shortage(smaller, NewHash, From, MyHash, SuccList);
        {_, []} -> join_op_shortage(smaller, NewHash, From, MyHash, SuccList);
        {Len, Bigger} ->
            {OtherServer, _} = lists:nth(Len, Bigger),
            gen_server:cast(OtherServer, {join_op_cast, NewHash, From}),
            SuccList
    end.

join_op_shortage(bigger, NewHash, From, MyHash, MySuccList) ->
    ResultSuccList = case MySuccList#succlist.bigger_len of
        0 ->
            MySuccList#succlist {
                bigger_len = 0,
                smaller = MySuccList#succlist.smaller ++ [{whereis(?MODULE), MyHash}]
            };
        _ ->
            MySuccList#succlist {
                bigger_len = MySuccList#succlist.bigger_len - 1,
                smaller = MySuccList#succlist.smaller ++ [{whereis(?MODULE), MyHash}]
            }
    end,
    join_op_1(bigger, NewHash, From, MySuccList, ResultSuccList);
join_op_shortage(smaller, NewHash, From, MyHash, MySuccList) ->
    ResultSuccList = MySuccList#succlist {
        bigger_len = MySuccList#succlist.bigger_len + 1,
        bigger = MySuccList#succlist.bigger ++ [{whereis(?MODULE), MyHash}]
    },
    join_op_1(smaller, NewHash, From, MySuccList, ResultSuccList).

join_op_1(S_or_B, NewHash, From, MySuccList, ResultSuccList) ->
    gen_server:reply(From, ResultSuccList),
    join_op_2(S_or_B, NewHash, From, MySuccList).

join_op_2(bigger, NewHash, From, MySuccList) ->
    {NewServer, _} = From,
    MySuccList#succlist {
        bigger_len = MySuccList#succlist.bigger_len + 1,
        bigger = [{NewServer, NewHash} | MySuccList#succlist.bigger]
    };
join_op_2(smaller, NewHash, From, MySuccList) ->
    {NewServer, _} = From,
    case MySuccList#succlist.bigger_len of
        0 ->
            MySuccList#succlist {
                bigger_len = 0,
                smaller = [{NewServer, NewHash} | MySuccList#succlist.bigger]
            };
        _ ->
            MySuccList#succlist {
                bigger_len = MySuccList#succlist.bigger_len - 1,
                smaller = [{NewServer, NewHash} | MySuccList#succlist.bigger]
            }
    end.


%%--------------------------------------------------------------------
%% lookup operation
%%--------------------------------------------------------------------
%% spawned funtion
lookup_op(Key, MyHash, From, SuccList) ->
    Hash = crypto:sha(term_to_binary(Key)),
    {S_or_B, SuccList1, Len} = select_succlist(Hash, MyHash, SuccList),
    case next(S_or_B, Hash, SuccList1, Len) of
        biggest                      -> lookup_op_biggest(Key, From, SuccList);
        self                         -> lookup_op_1(Key, From);
        {shortage, self}             -> lookup_op_1(Key, From);
        {shortage, {OtherServer, _}} -> gen_server:cast(OtherServer, {lookup_op_cast, Key, From});
        {error, Reason}              -> gen_server:reply(From, {whereis(?MODULE), {error, Reason}});
        {OtherServer, _}             -> gen_server:cast(OtherServer, {lookup_op_cast, Key, From})
    end.

lookup_op_biggest(Key, From, SuccList) ->
    case {SuccList#succlist.bigger_len, SuccList#succlist.bigger} of
        {0, _}  -> lookup_op_1(Key, From);
        {_, []} -> lookup_op_1(Key, From);
        {Len, Bigger} ->
            {OtherServer, _} = lists:nth(Len, Bigger),
            gen_server:cast(OtherServer, {lookup_op_cast, Key, From})
    end.

lookup_op_1(Key, From) ->
    case ets:lookup(store, Key) of
        [] -> gen_server:reply(From, {whereis(?MODULE), {error, {not_found, Key}}});
        [{Key, Value}] -> gen_server:reply(From, {whereis(?MODULE), {Key, Value}})
    end.


%%--------------------------------------------------------------------
%% put operation
%%--------------------------------------------------------------------
put_op(Key, Value, MyHash, SuccList) ->
    put_op_1(confirm(Key, MyHash, SuccList), Key, Value).

put_op_1(true, Key, Value) ->
    ets:insert(store, {Key, Value});
put_op_1(false, _, _) ->
    {error, not_me}.


%%--------------------------------------------------------------------
%% put operation
%%--------------------------------------------------------------------
call_op(Key, Module, Func, Args, MyHash, SuccList) ->
    call_op_1(confirm(Key, MyHash, SuccList), [Module, Func, Args]).

call_op_1(true, Args) ->
    apply(erlang, apply, Args);
call_op_1(false, _) ->
    {error, not_me}.


%%====================================================================
%% utilities
%%====================================================================
myhash() ->
    crypto:start(),
    crypto:sha_init(),
    crypto:sha(term_to_binary(node())).


next(_, NewHash, [{_, NewHash} | _], _) ->
    {error, equal_hash};
next(bigger, NewHash, [{_, Hash} | _], _) when NewHash > Hash ->
    self;
next(smaller, NewHash, [{_, Hash} | _], _) when NewHash < Hash ->
    biggest;
next(bigger, _, _, 0) ->
    self;
next(bigger, _, [], Len) when Len /= 0 ->
    {shortage, self};
next(smaller, _, SuccList, Len) when Len == 0 orelse SuccList == [] ->
    biggest;
next(S_or_B, NewHash, SuccList, Len) ->
    next_1(S_or_B, NewHash, SuccList, Len, 1).

next_1(_, _, [Peer | _], Len, Len) ->
    Peer;
next_1(_, _, [Peer], Len, Count) when Count /= Len->
    {shortage, Peer};

next_1(bigger, NewHash, [{Server, Hash1}, {_, Hash2} | _], _, _) when NewHash < Hash2 ->
    {Server, Hash1};
next_1(bigger, NewHash, [_, {Server, Hash} | Tail], Len, Count) when NewHash > Hash ->
    next_1(bigger, NewHash, [{Server, Hash} | Tail], Len, Count + 1);

next_1(smaller, NewHash, [{Server, Hash1}, {_, Hash2} | _], _, _) when NewHash > Hash2 ->
    {Server, Hash1};
next_1(smaller, NewHash, [_, {Server, Hash} | Tail], Len, Count) when NewHash < Hash ->
    next_1(smaller, NewHash, [{Server, Hash} | Tail], Len, Count + 1).


select_succlist(Hash1, Hash2, SuccList) when Hash1 > Hash2 ->
    {bigger, SuccList#succlist.bigger, SuccList#succlist.bigger_len};
select_succlist(Hash1, Hash2, SuccList) when Hash1 < Hash2 ->
    {smaller, SuccList#succlist.smaller, ?LENGTH_SUCCESSOR_LIST - SuccList#succlist.bigger_len}.


confirm(Key, MyHash, SuccList) ->
    Hash = crypto:sha(term_to_binary(Key)),
    confirm_1(Hash, MyHash, SuccList).

confirm_1(Hash, MyHash, _) when Hash == MyHash ->
    true;
confirm_1(Hash, MyHash, SuccList) when Hash > MyHash andalso (SuccList#succlist.bigger_len == 0 orelse SuccList#succlist.bigger == []) ->
    confirm_biggest(SuccList);
confirm_1(Hash, MyHash, SuccList) when Hash < MyHash andalso (?LENGTH_SUCCESSOR_LIST - SuccList#succlist.bigger_len == 0 orelse SuccList#succlist.smaller == []) ->
    confirm_biggest(SuccList);
confirm_1(Hash, MyHash, SuccList) ->
    confirm_2(Hash, MyHash, SuccList).

confirm_2(Hash, MyHash, SuccList) when Hash > MyHash ->
    {_, NextHash} = lists:nth(1, SuccList#succlist.bigger),
    if
        Hash < NextHash  -> true;
        Hash >= NextHash -> false
    end;
confirm_2(Hash, MyHash, SuccList) when Hash < MyHash ->
    {_, NextHash} = lists:nth(1, SuccList#succlist.smaller),
    if
        Hash > NextHash  -> true;
        Hash =< NextHash -> confirm_biggest(SuccList) 
    end.

confirm_biggest(SuccList) when SuccList#succlist.bigger == [] orelse SuccList#succlist.bigger_len == 0 ->
    true;
confirm_biggest(_) ->
    false.

