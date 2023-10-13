.PHONY: setup
setup:
	python -m venv .venv \
	&& source ./.venv/bin/activate \
	&& python -m pip install --upgrade -r ./acit/requirements.txt \
	&& python -m pip install coverage pyink

.PHONY: test
test: format
	coverage run ci/run_tests.py && coverage xml

.PHONY: format
format:
	pyink \
	  --pyink-indentation=2 \
	  --line-length=80 \
	  --pyink-use-majority-quotes \
	  acit/

