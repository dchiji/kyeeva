-record(state, {
    myhash :: binary(),
    succlist :: tuple() 
}).

-record(succlist, {
    bigger_len :: integer(),
    bigger :: list(),
    smaller :: list()
}).

-define(LENGTH_SUCCESSOR_LIST, 3).
