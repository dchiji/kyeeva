
-module(chord_man).
-behaviour(gen_server).

-include("../include/common_chord.hrl").

%% API
-export([start/2,
        start/3,
        successor/0,
        successor/1,
        find/2,
        set/1]).

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
start(Hash, SuccList) ->
    start(Hash, SuccList, ?DEFAULT_MAX_LEVEL).

start(Hash, SuccList, N) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Hash, SuccList, N], []).


successor() ->
    gen_server:call(?MODULE, successor).

successor(Level) ->
    gen_server:call(?MODULE, {successor, Level}).


find(Level, Hash) ->
    gen_server:call(?MODULE, {find, Level, Hash}).


set(NewSuccList) ->
    gen_server:call(?MODULE, {set, NewSuccList}).


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
init([Hash, SuccList, N]) ->
    {ok, #state_man{myhash=Hash, succlists=[SuccList | lists:duplicate(N - 1, {succlist, [], []})]}}.


%%--------------------------------------------------------------------
%% Function: handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                                {reply, Reply, State, Timeout} |
%%                                                {noreply, State} |
%%                                                {noreply, State, Timeout} |
%%                                                {stop, Reason, Reply, State} |
%%                                                {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(succlist, _, State) ->
    {reply, lists:nth(1, State#state_man.succlists), State};

handle_call({succlist, Level}, _, State) ->
    {reply, lists:nth(Level, State#state_man.succlists), State};

handle_call({find, Level, Hash}, _, State) ->
    {reply, find(Hash, State#state_man.myhash, lists:nth(Level, State#state_man.succlists)), State};

handle_call({set, NewSuccList}, _, State) ->
    {reply, ok, State#state_man{succlists=lists:map(fun({1, _}) -> NewSuccList; ({_, X}) -> X end, lists:zip(lists:seq(1, length(State#state_man.succlists)), State#state_man.succlists))}};

handle_call({set, Level, NewSuccList}, _, State) ->
    {reply, ok, State#state_man{succlists=lists:map(fun({N, _}) when N == Level -> NewSuccList; ({_, X}) -> X end, lists:zip(lists:seq(1, length(State#state_man.succlists)), State#state_man.succlists))}};

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
%% utilities
%%====================================================================
find(Hash, MyHash, SuccList) ->
    case next(MyHash, Hash, SuccList) of
        {error, Reason} -> {error, Reason};
        biggest         -> find_biggest(SuccList);
        self            -> self;
        Peer            -> Peer
    end.


%% the first argument must be 'smaller'
find_biggest(SuccList) ->
    case SuccList#succlist.bigger of
        []     -> self;
        Bigger -> lists:nth(length(Bigger), Bigger)
    end.


%% bigger
next(MyHash, NewHash, {succlist, [{_, NewHash}=Peer | _], _}) when MyHash =< NewHash ->
    Peer;
next(MyHash, NewHash, {succlist, [{_, Hash} | _], _}) when (MyHash =< NewHash) and (NewHash > Hash) ->
    self;
next(MyHash, NewHash, {succlist, [], _}) when MyHash =< NewHash ->
    self;
next(MyHash, NewHash, {succlist, SuccList, _}) when MyHash =< NewHash ->
    next_1(MyHash, NewHash, SuccList);

%% smaller
next(MyHash, NewHash, {succlist, _, [{_, NewHash}=Peer | _]}) when MyHash >= NewHash ->
    Peer;
next(MyHash, NewHash, {succlist, _, [{_, Hash} | _]}) when (MyHash >= NewHash) and (NewHash < Hash) ->
    biggest;
next(MyHash, NewHash, {succlist, _, []}) when MyHash >= NewHash ->
    biggest;
next(MyHash, NewHash, {succlist, _, SuccList}) when MyHash >= NewHash ->
    next_1(MyHash, NewHash, SuccList).


next_1(_, _, [Peer]) ->
    Peer;

%% bigger
next_1(MyHash, NewHash, [{Server, Hash1}, {_, Hash2} | _]) when (MyHash =< NewHash) and (NewHash < Hash2) ->
    {Server, Hash1};
next_1(MyHash, NewHash, [_, {Server, Hash} | Tail]) when (MyHash =< NewHash) and (NewHash > Hash) ->
    next_1(MyHash, NewHash, [{Server, Hash} | Tail]);

%% smaller
next_1(MyHash, NewHash, [{Server, Hash1}, {_, Hash2} | _]) when (MyHash >= NewHash) and (NewHash > Hash2) ->
    {Server, Hash1};
next_1(MyHash, NewHash, [_, {Server, Hash} | Tail]) when (MyHash >= NewHash) and (NewHash < Hash) ->
    next_1(MyHash, NewHash, [{Server, Hash} | Tail]).
