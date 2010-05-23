
-module(store_server).
-behaviour(gen_server).

-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([start/0,
        put/2,
        put/3,
        get/1,
        range/2]).

-export([range_op/3]).

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
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


put(Key, Value) ->
    put(Key, Value, crypto:sha(term_to_binary(Key))).

put(Key, Value, Hash) ->
    gen_server:call(?MODULE, {put_op, Key, Value, Hash}).


get(Key) ->
    gen_server:call(?MODULE, {get_op, Key}).


range(Hash1, Hash2) ->
    gen_server:call(?MODULE, {range_op, Hash1, Hash2}).


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
init([]) ->
    Ref = make_ref(),
    spawn((fun(To) -> fun() -> To ! {Ref, ok, ets:new(store_table, [named_table, public, ordered_set])}, timer:sleep(infinity) end end)(self())),
    receive
        {Ref, ok, Table} -> {ok, [Table]}
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
handle_call({put_op, Key, Value, Hash}, _, State) ->
    {reply, ets:insert(store_table, {Key, Value, Hash}), State};
handle_call({get_op, Key}, _, State) ->
    {reply, ets:lookup(store_table, Key), State};
handle_call({range_op, Hash1, Hash2}, _, [Table]=State) ->
    {reply, range_op(Table, Hash1, Hash2), State};
handle_call(_, _, State) ->
    {noreply, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
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
%% split operation
%%--------------------------------------------------------------------
range_op(Table, Hash1, Hash2) when Hash1 < Hash2 ->
    ets:select(Table, ets:fun2ms(fun({_, _, Hash}=Element) when Hash1 =< Hash, Hash < Hash2 -> Element end));
range_op(Table, Hash1, Hash2) when Hash1 > Hash2 ->
    ets:select(Table, ets:fun2ms(fun({_, _, Hash}=Element) when (Hash1 =< Hash) or (Hash < Hash2) -> Element end)).
