all: unittest coverage

egg:
	PYTHONPATH=$(PYTHONPATH):./cis python setup.py bdist_egg

unittest:
	PYTHONPATH=$(PYTHONPATH):./cis py.test -s

singletest:
	PYTHONPATH=$(PYTHONPATH):./cis nose2 -s tests/ut $(TEST)

clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	rm -rf build
	rm -rf dist
	rm -rf cis.egg-info
