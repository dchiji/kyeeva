-record(state, {
    myhash :: binary(),
    manager :: pid(),
    store :: pid()
}).

-record(state_man, {
    myhash :: binary(),
    succlists :: list(tuple())
}).

-record(succlist, {
    bigger :: list(),
    smaller :: list()
}).

-define(LENGTH_SUCCESSOR_LIST, 3).
-define(DEFAULT_MAX_LEVEL, 1).
