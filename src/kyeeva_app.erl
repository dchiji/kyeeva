
-module(kyeeva_app).
-behaviour(application).

%% API
-export([start/0,
        stop/0,
        put/1,
        put/2,
        get/1,
        get/2,
        get/3]).

%% application callbacks
-export([start/2,
        stop/1]).


%%====================================================================
%% API
%%====================================================================
start() ->
    crypto:start(),
    crypto:sha_init(),
    case application:start(kyeeva) of
        ok -> ok;
        {error, Reason} -> io:format("boot error: ~p~n", [Reason])
    end.


stop() ->
    ok.


put(AttrList) ->
    put(crypto:sha(term_to_binary({node(), now()})), AttrList).

put(GUID, AttrList) ->
    chord_server:call(GUID, sg_server, put, [GUID, AttrList]).


get(Key) ->
    get(Key, [value]).

get(Key, AttributeList) ->
    sg_server:get(Key, AttributeList).

get(Key0, Key1, AttributeList) ->
    sg_server:get(Key0, Key1, AttributeList).


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
