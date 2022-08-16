.PHONY: lock install test lint format # build-package build-docs build publish
.DEFAULT_GOAL := help

# Install
lock:
	poetry lock

install:
	poetry install

## Test
test: 
	pytest -vv
	pytest --cov --cov-report html
	pytest --mccabe -vv

## Lint
lint:
	flake8 aggregator tests || exit 1
	isort --check-only --diff aggregator tests || exit 1
	black --check aggregator tests || exit 1
	dmypy run -- aggregator tests || exit 1

# pydocstyle src tests || exit 1	

format:
	isort aggregator tests
	black aggregator tests

## Build
# build-package:
#	poetry build

# build-docs:
#	pip install -r docs/requirements.txt
#	make -C docs html

# build: build-package

# publish: build
#	poetry publish -u __token__ -p '${PYPI_PASSWORD}' --no-interaction
