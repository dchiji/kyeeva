.SUFFIXES: .erl .beam .yrl

.erl.beam:
	erlc -W +'{parse_transform, smart_exceptions}' $<

.yrl.erl:
	erlc -W +'{parse_transform, smart_exceptions}' $<

ERL = erl 

MODS = skipgraph test mc_cover

all: compile

compile: ${MODS:%=%.beam}

test: compile
	${ERL} -sname test +P 400000 -s test test

clean:
	rm -rf *.beam erl_crash.dump
