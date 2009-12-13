.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W +'{parse_transform, smart_exceptions}' $<

.yrl.erl:
	erlc -W +'{parse_transform, smart_exceptions}' $<

ERL = erl 

MODS = skipgraph test

all: compile

compile: ${MODS:%=%.beam}

speciall.beam: speciall.erl
	${ERL} -Dflag1 -W0 speciall.erl

test: compile
	${ERL} -s test test

clean:
	rm -rf *.beam erl_crash.dump
