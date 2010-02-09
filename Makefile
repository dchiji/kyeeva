.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W +'{parse_transform, smart_exceptions}' $<

.yrl.erl:
	erlc -W +'{parse_transform, smart_exceptions}' $<

ERL = erl 

MODS = src/skipgraph src/join src/lookup src/remove src/util test/test src/mcfe test/benchmark

all: compile

compile: ${MODS:%=%.beam}

test: compile
	${ERL} -sname test +P 4000000 -s test test

benchmark: compile
	${ERL} -sname benchmark +P 4000000 -s benchmark benchmark

clean:
	rm -rf *.beam erl_crash.dump
