.SUFFIXES: .erl .beam .yrl
.erl.beam:
#	erlc -Ddebug -W3 +native +'{parse_transform, smart_exceptions}' $<
	erlc -W3 +native +'{parse_transform, smart_exceptions}' $<
.yrl.erl:
#	erlc -Ddebug -W3 +native +'{parse_transform, smart_exceptions}' $<
	erlc -W3 +native +'{parse_transform, smart_exceptions}' $<


ERL = erl
MODS = src/sg src/sg_join src/sg_lookup src/sg_remove src/util_lock src/util src/util_mvector test/test
#MODS = src/util_lock src/util src/util_mvector src/sg_join test/mock_sg_join
all: compile
compile: ${MODS:%=%.beam}


test: compile
	${ERL} -sname test +P 4000000 -noshell -s test test
ctest: compile
	${ERL} -sname test +P 4000000 -noshell -s concurrent_join_test test
mock: compile
	${ERL} -sname test +P 4000000 -noshell -s mock_sg_join start
benchmark: compile
	${ERL} -sname benchmark +P 4000000 -noshell -s benchmark benchmark
clean:
	rm -f ./*.beam ./erl_crash.dump; rm -f src/*.beam src/erl_crash.dump
