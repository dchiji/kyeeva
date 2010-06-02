-module(udp_server).
-behaviour(gen_server).

%% API
-export([start/0,
        to/2,
        server/0]).

-export([output/3]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {
        socket :: gen_udp:socket(),
        port :: integer()
    }).

-define(PORT, 10100).
-define(UDP_SOCKET_OPTIONS, [binary]).
-define(WAIT_TIME, 1000).


%%====================================================================
%% API
%%====================================================================
start() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [?PORT], []).


to(Addr, Port) ->
    ets:insert(event_tab, {'__to__', {Addr, Port}}).


server() ->
    whereis(?MODULE).


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
init([Port]) ->
    spawn(fun() -> ets:new(event_tab, [public, bag, named_table]), timer:sleep(infinity) end),
    case catch gen_udp:open(Port, ?UDP_SOCKET_OPTIONS) of
        {ok, Socket} -> {ok, #state{socket=Socket, port=Port}};
        {error, Reason} -> {stop, Reason}
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
handle_call({message, Ref, Time, Message}, _, State) ->
    ets:insert(event_tab, {Ref, Time, Message}),
    {reply, ok, State};
handle_call({output, Ref}, _, State) ->
    spawn(?MODULE, output, [Ref, State#state.socket, ?WAIT_TIME]),
    {reply, ?WAIT_TIME, State};
handle_call(_, _, State) ->
    {noreply, ok, State}.

output(Ref, Socket, Time) ->
    timer:sleep(Time),
    MessageList = ets:lookup(event_tab, Ref),
    ToList = ets:lookup(event_tab, '__to__'),
    [send(To, MessageList, Socket) || To <- ToList].

send({Addr, Port}, MessageList, Socket) ->
    %% TODO
    Packet = term_to_binary(MessageList),
    gen_udp:send(Socket, Addr, Port, Packet).


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
