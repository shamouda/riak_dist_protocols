REBAR = $(shell pwd)/rebar3
.PHONY: rel 

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean

shell: rel
	export NODE_NAME=simple_kv@127.0.0.1 ; \
	export COOKIE=secret ; \
	export ROOT_DIR_PREFIX=$$NODE_NAME/ ; \
	_build/default/rel/simple_kv/bin/simple_kv console ${ARGS}

shell2: rel
	export NODE_NAME=simple_kv2@127.0.0.1 ; \
	export COOKIE=secret ; \
	export ROOT_DIR_PREFIX=$$NODE_NAME/ ; \
	_build/default/rel/simple_kv/bin/simple_kv console -riak_core handoff_port 9000 ${ARGS}

rel:
	$(REBAR) release

xref: compile
	${REBAR} xref skip_deps=true

dialyzer:
	${REBAR} dialyzer
