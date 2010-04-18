-module(kyeeva_app).
-behaviour(application).

%% API
-export([start/0,
        stop/0]).

%% application callbacks
-export([start/2,
        stop/1]).


%%====================================================================
%% API
%%====================================================================
start() ->
    case application:start(kyeeva) of
        ok -> ok;
        {error, Reason} -> io:format("boot error: ~p~n", [Reason])
    end.


stop() ->
    ok.


%%====================================================================
%% application callbacks
%%====================================================================
start(_, _) ->
    {ok, MaxR} = application:get_env(kyeeva, max_r),
    {ok, MaxT} = application:get_env(kyeeva, max_t),
    {ok, InitNode} = application:get_env(kyeeva, initnode),
    kyeeva_sup:start(MaxR, MaxT, InitNode).


stop(_) ->
    ok.
