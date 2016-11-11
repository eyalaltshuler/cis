all: unittest

unittest:
	PYTHONPATH=$(PYTHONPATH):/home/eyal/work/cis/src py.test -s

singletest:
	PYTHONPATH=$(PYTHONPATH):/home/eyal/work/cis/src py.test -s $(TEST)

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	
