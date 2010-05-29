-record(pstate, {
        parent_key :: atom() | list(),
        mvector :: binary(),
        smaller :: list(tuple()),
        bigger :: list(tuple())
    }).

-ifdef(debug).
    -define(LEVEL_MAX, 8).
-else.
    %-define(LEVEL_MAX, 8).
    -define(LEVEL_MAX, 16).
    %-define(LEVEL_MAX, 32).
    %-define(LEVEL_MAX, 64).
    %-define(LEVEL_MAX, 128).
-endif.

-define(SERVER_MODULE, sg_server).
