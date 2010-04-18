-record(pstate, {
        parent_key :: atom() | list(),
        mvector :: binary(),
        smaller :: list(tuple()),
        bigger :: list(tuple())
    }).

-ifdef(debug).
    -define(LEVEL_MAX, 4).
-else.
    %-define(LEVEL_MAX, 8).
    -define(LEVEL_MAX, 16).
    %-define(LEVEL_MAX, 32).
    %-define(LEVEL_MAX, 64).
    %-define(LEVEL_MAX, 128).
-endif.

-ifdef(debug).
    -define(SERVER_MODULE, mock_sg_join).
-else.
    -define(SERVER_MODULE, sg_server).
-endif.
