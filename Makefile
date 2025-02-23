.PHONY: lock install test lint format # build-package build-docs build publish
.DEFAULT_GOAL := help

# Install
lock:
	poetry lock

install:
	poetry install

## Test
test_all: 
	poetry run pytest --mccabe --cov --cov-report html -vv

test_unit:
	poetry run pytest -vv -m "unit and not db"

test_db:
	poetry run pytest -vv -m "unit or mock"


## Lint
lint:
	poetry run flake8 aggregator tests || exit 1
	poetry run isort --check-only --diff aggregator tests || exit 1
	poetry run black --check aggregator tests || exit 1
	poetry run dmypy run -- aggregator tests -vv || exit 1

# pydocstyle src tests || exit 1	

format:
	poetry run isort aggregator tests
	poetry run black aggregator tests

## Build
# build-package:
#	poetry build

# build-docs:
#	pip install -r docs/requirements.txt
#	make -C docs html

# build: build-package

# publish: build
#	poetry publish -u __token__ -p '${PYPI_PASSWORD}' --no-interaction
