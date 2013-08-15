
default: all

all:
	@echo Nothing to do!
	@echo You can run tests with: make test

PYTHON=python

runtests: all runtests-cmdline

runtests-cmdline: all
	t/test.sh

test: all
	./wvtestrun $(MAKE) PYTHON=$(PYTHON) runtests

clean:
	rm -f *.pyc lib/*.pyc
	if [ -e t/tmp ]; then rm -r t/tmp; fi
