PYTHON ?= python3
VENV_PYTHON ?= .venv/bin/python

.PHONY: setup test lint format init-db metrics ingest-eml ingest-mbox parse-issues label-articles build-embeddings export-parquet run-backfill full-refresh

setup:
	$(PYTHON) -m venv .venv
	$(VENV_PYTHON) -m pip install -e ".[dev]"

test:
	$(VENV_PYTHON) -m pytest

lint:
	$(VENV_PYTHON) -m ruff check src tests

format:
	$(VENV_PYTHON) -m ruff format src tests

init-db:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli init-db

metrics:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli metrics

ingest-eml:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli ingest-eml $(PATH)

ingest-mbox:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli ingest-mbox $(PATH)

parse-issues:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli parse-issues

label-articles:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli label-articles --force

build-embeddings:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli build-embeddings --force

export-parquet:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli export-parquet

run-backfill:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli run-backfill $(PATH) --source $(SOURCE)

full-refresh:
	$(VENV_PYTHON) -m tldr_crypto_finance.cli full-refresh --force-parse --force-label
