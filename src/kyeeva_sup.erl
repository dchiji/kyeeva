-module(kyeeva_sup).
-behaviour(supervisor).

%% API
-export([start/0,
        start/3]).

%% supervisor callbacks
-export([init/1]).

-define(DEFAULT_MAXR, 3).
-define(DEFAULT_MAXT, 60).


start() ->
    start(?DEFAULT_MAXR, ?DEFAULT_MAXT, nil).

start(MaxR, MaxT, InitNode) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [MaxR, MaxT, InitNode]).


init([MaxR, MaxT, InitNode]) ->
    {ok, {{one_for_all, MaxR, MaxT}, child_spec(InitNode)}}.


child_spec(InitNode) ->
    [chord_server(InitNode), sg_server(InitNode)].

chord_server(InitNode) ->
    ID         = chord_server,
    StartFunc  = {chord_server, start, [InitNode]},
    Restart    = permanent,
    Shutdown   = brutal_kill,
    Type       = worker,
    Modules    = [chord_server],
    {ID, StartFunc, Restart, Shutdown, Type, Modules}.

sg_server(InitNode) ->
    ID         = sg_server,
    StartFunc  = {sg_server, start, [InitNode]},
    Restart    = permanent,
    Shutdown   = brutal_kill,
    Type       = worker,
    Modules    = [sg_server],
    {ID, StartFunc, Restart, Shutdown, Type, Modules}.

