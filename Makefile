.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W +'{parse_transform, smart_exceptions}' $<

.yrl.erl:
	erlc -W +'{parse_transform, smart_exceptions}' $<

ERL = erl 

MODS = skipgraph test

all: compile

compile: ${MODS:%=%.beam}

test: compile
	${ERL} +P 400000 -s test test

clean:
	rm -rf *.beam erl_crash.dump
