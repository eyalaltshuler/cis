all: unittest coverage

unittest:
	PYTHONPATH=$(PYTHONPATH):./cis py.test -s

singletest:
	PYTHONPATH=$(PYTHONPATH):./cis py.test -k $(TEST)

coverage:
	PYTHONPATH=$(PYTHONPATH):./cis py.test --cov=/home/eyal/work/cis/src

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
