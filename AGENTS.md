# AGENTS

This repository is a local-first Python pipeline for finance and newsletter intelligence. The core workflow is mailbox export or live sync, then parse, label, retrieve, and export.

## Project Layout

- `src/tldr_crypto_finance/cli.py` contains the Typer CLI.
- `src/tldr_crypto_finance/db` contains the DuckDB schema and curated views.
- `src/tldr_crypto_finance/ingestion` contains MBOX, EML, Gmail, IMAP, and checkpoint logic.
- `src/tldr_crypto_finance/parsing` contains HTML cleanup, section splitting, article splitting, link extraction, and sponsor filtering.
- `src/tldr_crypto_finance/labeling` contains taxonomy loading, rule labels, optional zero-shot hooks, entities, embeddings, and review helpers.
- `src/tldr_crypto_finance/retrieval` contains SQL retrieval, ranking, briefs, and context export.
- `src/tldr_crypto_finance/pipelines` contains orchestration for backfill, refresh, and sync.
- `tests` contains unit and workflow-level tests with synthetic fixtures.
- `configs` contains taxonomy, sponsor rules, sender profiles, and watchlists.

## Setup Commands

```bash
python3 -m venv .venv
source .venv/bin/activate
.venv/bin/python -m pip install -e ".[dev]"
cp .env.example .env
.venv/bin/python -m tldr_crypto_finance.cli init-db
```

## Test Commands

```bash
make test
.venv/bin/python -m pytest
```

## Lint And Format

```bash
make lint
make format
.venv/bin/python -m ruff check src tests
.venv/bin/python -m ruff format src tests
```

## Main CLI Workflows

- Historical EML backfill:

```bash
.venv/bin/python -m tldr_crypto_finance.cli run-backfill /path/to/eml_dir --source eml
```

- Historical MBOX backfill:

```bash
.venv/bin/python -m tldr_crypto_finance.cli run-backfill /path/to/archive.mbox --source mbox
```

- Manual staged flow:

```bash
.venv/bin/python -m tldr_crypto_finance.cli ingest-eml /path/to/eml_dir
.venv/bin/python -m tldr_crypto_finance.cli parse-issues
.venv/bin/python -m tldr_crypto_finance.cli label-articles --force
.venv/bin/python -m tldr_crypto_finance.cli build-embeddings --force
.venv/bin/python -m tldr_crypto_finance.cli export-parquet
```

- Query and briefing:

```bash
.venv/bin/python -m tldr_crypto_finance.cli query-articles --topic crypto_markets --output markdown
.venv/bin/python -m tldr_crypto_finance.cli search-similar "stablecoin liquidity reserve backing" --output markdown
.venv/bin/python -m tldr_crypto_finance.cli risk-brief stablecoin --topic crypto_markets
```

- Live sync:

```bash
.venv/bin/python -m tldr_crypto_finance.cli sync-gmail
.venv/bin/python -m tldr_crypto_finance.cli sync-imap
.venv/bin/python -m tldr_crypto_finance.cli run-sync --gmail --imap
```

## What Counts As Done

- The code path you changed is covered by tests or direct command validation.
- `ruff check` passes.
- The default local path works without optional ML or cloud dependencies.
- Non-trivial functions, methods, and classes have docstrings.
- CLI commands and documentation match the implemented behavior.
- New parsing, ingestion, dedupe, labeling, retrieval, or database-write logic mentions key assumptions or side effects in its docstring.

## Code Style Rules

- Add docstrings to every non-trivial function, method, and class.
- Skip docstrings only for trivial one-line helpers whose behavior is obvious from the name, signature, and type hints.
- Keep docstrings short, concrete, and useful.
- Prefer clear control flow and explicit data movement over abstraction for its own sake.
- Keep the default path local and lightweight.
- When touching parsing, ingestion, deduplication, labeling, retrieval, or database writes, document important assumptions and side effects.
- Do not add placeholder commands or dead modules. If a command exists, it should run or fail clearly for a real reason such as missing optional credentials.
