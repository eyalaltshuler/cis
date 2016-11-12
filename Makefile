all: unittest coverage

unittest:
	PYTHONPATH=$(PYTHONPATH):/home/eyal/work/cis/src py.test -s

singletest:
	PYTHONPATH=$(PYTHONPATH):/home/eyal/work/cis/src py.test -k $(TEST)

coverage:
	PYTHONPATH=$(PYTHONPATH):/home/eyal/work/cis/src py.test --cov=/home/eyal/work/cis/src

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
