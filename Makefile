PY ?= python

.PHONY: generate lint test test-contract build

generate:
	PATH="$$(dirname "$$(command -v $(PY))"):$$PATH" sh scripts/generate.sh

lint:
	$(PY) -m ruff check
	$(PY) -m ruff format --check
	$(PY) -m mypy

test:
	$(PY) -m pytest

test-contract:
	$(PY) -m pytest tests/contract

build:
	$(PY) -m build
