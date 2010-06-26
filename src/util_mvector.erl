
-module(util_mvector).

-export([make/0,
        is_equal/2,
        is_equal/3,
        nth/2,
        io_binary/1]).

-include("../include/common_sg.hrl").


make() ->
    {A1, A2, A3} = now(),
    random:seed(erlang:phash2(A1), erlang:phash2(A2), erlang:phash2(A3)),
    make_lsb_zero(random:uniform(256) - 1).

make(MVector, 0.0) ->
    MVector;
make(MVector, N) ->
    make(<<(random:uniform(256) - 1):8, MVector/binary>>, N - 1).

%% 最下位ビットを0に変換する
make_lsb_zero(N) when N rem 2 == 0 ->
    make(<<N>>, ?LEVEL_MAX / 8 - 1);
make_lsb_zero(N) when N rem 2 == 1 ->
    M = N - 1,
    make(<<M>>, ?LEVEL_MAX / 8 - 1).


nth(Level, MVector) ->
    Level_1 = Level - 1,
    TailN = ?LEVEL_MAX - Level_1 - 1,
    <<_:TailN, Bit:1, _:Level_1>> = MVector,
    Bit.


is_equal(MVector1, MVector2) ->
    MVector1 == MVector2.

is_equal(MVector1, MVector2, Level) ->
    B1 = nth(Level, MVector1),
    B2 = nth(Level, MVector2),
    B1 == B2.


io_binary(MV) ->
    io_binary_1(MV, ?LEVEL_MAX).

io_binary_1(_, 0) -> ok;
io_binary_1(MV, Size) ->
    TailN = Size - 8,
    <<Byte, MV1/binary>> = MV,
    io_binary_1(MV1, TailN).
