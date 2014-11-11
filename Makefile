REPORTER ?= list
SRC = $(shell find lib -name "*.js" -type f | sort)
TESTSRC = $(shell find test -name "*.js" -type f | sort)

default: test

lint: $(SRC) $(TESTSRC)
	@node_modules/.bin/jshint --reporter node_modules/jshint-stylish/stylish.js $^

test-unit: lint
	@node node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		--ui bdd \
		test/*-test.js test/types/*-test.js

test-integration: lint
	@node node_modules/.bin/mocha \
		--reporter spec \
		--ui bdd \
		test/integration/*-test.js

test-cov:
	@node node_modules/.bin/mocha \
		-r jscoverage test/

test-cov-html:
	@node node_modules/.bin/mocha \
		-r jscoverage \
		--covout=html test/

test: test-unit test-integration

.PHONY: test test-cov test-cov-html
