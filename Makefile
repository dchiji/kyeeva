.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W +'{parse_transform, smart_exceptions}' $<

.yrl.erl:
	erlc -W +'{parse_transform, smart_exceptions}' $<

ERL = erl 

MODS = src/skipgraph src/join src/lookup src/remove src/util test/test src/mcfe test/benchmark test/concurrent_join_test

all: compile

compile: ${MODS:%=%.beam}

test: compile
	${ERL} -sname test +P 4000000 -s test test

ctest: compile
	${ERL} -sname test +P 4000000 -s concurrent_join_test test

benchmark: compile
	${ERL} -sname benchmark +P 4000000 -s benchmark benchmark

clean:
	cd ebin; rm -rf *.beam erl_crash.dump; cd ..
