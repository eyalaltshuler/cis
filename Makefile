all: unittest coverage

unittest:
	PYTHONPATH=$(PYTHONPATH):./cis py.test -s

singletest:
	PYTHONPATH=$(PYTHONPATH):./cis nose2 -s tests/ut $(TEST)

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
