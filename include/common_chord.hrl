-record(state, {
    myhash :: binary(),
    manager :: pid() 
}).

-record(state_man, {
    myhash :: binary(),
    succlists :: tuple()
}).

-record(succlist, {
    bigger :: list(),
    smaller :: list()
}).

-define(LENGTH_SUCCESSOR_LIST, 3).
-define(DEFAULT_MAX_LEVEL, 1).
