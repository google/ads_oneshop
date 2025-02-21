.PHONY: clean
clean:
	rm -rf ./.venv ./gtech-* ./dist/

.PHONY: setup
setup:
	python -m venv .venv

.PHONY: install_dependencies
install_dependencies: setup
	source ./.venv/bin/activate \
	&& python -m pip install --require-hashes -r requirements.txt \

.PHONY: build_dependencies
build_dependencies: install_dependencies
	source ./.venv/bin/activate \
	&& python -m pip install --require-hashes -r requirements_dev.txt \

# For editable installs, src/gen_bq_schema/__init__.py needs to remain undeleted
.PHONY: dev
dev: build_dependencies
	source ./.venv/bin/activate \
	&& git update-index --assume-unchanged ./src/gen_bq_schema/__init__.py \
	&& python -m pip install coverage pyink \
	&& python -m pip install -e .

.PHONY: update_requirements
update_requirements: clean setup
	rm -rf requirements*.txt \
	&& source ./.venv/bin/activate \
	&& python -m pip install pip-tools \
	&& pip-compile --only-build-deps --build-deps-for wheel --allow-unsafe --generate-hashes -o requirements_dev.txt pyproject.toml \
	&& pip-compile --all-extras --generate-hashes -o requirements.txt pyproject.toml

.PHONY: format
format: dev
	source ./.venv/bin/activate \
	&& pyink \
	  --unstable \
	  --pyink-indentation=2 \
	  --exclude='_pb2' \
	  --line-length=80 \
	  --pyink-use-majority-quotes \
	  src/acit/ \
	  setup.py

.PHONY: test
test: format
	source ./.venv/bin/activate \
	&& (cd src && coverage run ../ci/run_tests.py && coverage xml)

.PHONY: build
build: build_dependencies
	source ./.venv/bin/activate \
	&& python -m build -x -n

.PHONY: install
install: build
	source ./.venv/bin/activate \
	&& python -m pip install --force-reinstall --no-deps --no-index dist/gtech_oneshop-*-py3-*-*.whl
