# TLDR Crypto Finance

This repository builds a local pipeline for finance and crypto newsletter email. It ingests mailbox exports or mailbox syncs, splits each issue into article-level records, filters sponsored content, assigns labels, and stores the results in DuckDB plus Parquet outputs.

Newsletter email captures what analysts, traders, researchers, and operators were paying attention to at a given time. It also preserves framing, source links, sponsor blocks, repeated narratives, and tone changes that are often lost in headline feeds.

## Why This Shape

- DuckDB is the default database. It runs well on a laptop and keeps querying simple.
- Parquet exports make the data easy to inspect outside the CLI.
- Most email parsing stays in the Python standard library.
- BeautifulSoup handles HTML cleanup.
- Typer provides the CLI.
- Gmail sync is part of the default install. IMAP sync is supported for generic mailboxes. Zero-shot labeling and sentence-transformer embeddings stay behind the ML extras.

## Setup

- Create and activate a virtual environment.
- Install the project in editable mode with the development extras.
- Copy `.env.example` to `.env` and fill in the paths or credentials you need.
- Initialize the database before the first run.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
cp .env.example .env
python -m tldr_crypto_finance.cli init-db
```

## Quickstart

- If you already have `.eml` exports, the shortest path is a full backfill:

```bash
python -m tldr_crypto_finance.cli run-backfill tests/fixtures --source eml
```

- Query the resulting article store:

```bash
python -m tldr_crypto_finance.cli query-articles --topic crypto_markets --output markdown
```

- Generate a short brief:

```bash
python -m tldr_crypto_finance.cli risk-brief stablecoin --topic crypto_markets
```

## How To Use

- Create the environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

- Fill in `.env` from `.env.example`. The base setup only needs the database and data directories. Gmail needs OAuth credentials. IMAP needs host, username, and password.

- Initialize the database:

```bash
python -m tldr_crypto_finance.cli init-db
```

- Ingest mailbox exports. Use one of these:

```bash
python -m tldr_crypto_finance.cli ingest-eml /path/to/eml_directory
python -m tldr_crypto_finance.cli ingest-mbox /path/to/archive.mbox
```

- Parse issues into sections, article blocks, and links:

```bash
python -m tldr_crypto_finance.cli parse-issues
```

- Label article blocks and extract entities:

```bash
python -m tldr_crypto_finance.cli label-articles --force
```

- Build default local embeddings for similarity search:

```bash
python -m tldr_crypto_finance.cli build-embeddings --force
```

- Export curated Parquet outputs:

```bash
python -m tldr_crypto_finance.cli export-parquet
```

- Run a structured retrieval query:

```bash
python -m tldr_crypto_finance.cli query-articles --topic crypto_markets --output markdown
```

- Search by similarity:

```bash
python -m tldr_crypto_finance.cli search-similar "stablecoin liquidity reserve backing" --output markdown
```

- Export a JSON or markdown context bundle for later analysis:

```bash
python -m tldr_crypto_finance.cli export-context --topic crypto_markets --output json
```

- Open the notebook for exploration:

```bash
jupyter notebook notebooks/01_exploration.ipynb
```

- If something looks wrong, inspect these first:
  - `raw_messages` for ingestion problems.
  - `newsletter_issues`, `sections`, and `article_blocks` for parse quality.
  - `article_labels` and `manual_review_queue` for labeling gaps.
  - `runs` for pipeline run history.
  - `v_parse_quality_by_newsletter`, `v_low_confidence_labels`, and `v_duplicates` for targeted cleanup work.

## Common Flows

- Historical backfill from mailbox exports in one command:

```bash
python -m tldr_crypto_finance.cli run-backfill /path/to/eml_directory --source eml
python -m tldr_crypto_finance.cli run-backfill /path/to/archive.mbox --source mbox
```

- Query the resulting database for recent regulation or crypto coverage:

```bash
python -m tldr_crypto_finance.cli query-articles --topic regulation --output markdown
python -m tldr_crypto_finance.cli query-articles --topic crypto_markets --output markdown
```

- Generate a short brief for a risk topic:

```bash
python -m tldr_crypto_finance.cli risk-brief custody --topic crypto_markets
```

- Incremental mailbox sync:

```bash
python -m tldr_crypto_finance.cli sync-gmail
python -m tldr_crypto_finance.cli run-sync
python -m tldr_crypto_finance.cli run-sync --imap
python -m tldr_crypto_finance.cli run-sync --no-gmail --imap
```

## Retrieval

Retrieval works in layers. `query-articles` uses SQL filters on time, topic, sender, domain, asset class, and risk type. `search-similar` ranks filtered candidates with stored embeddings when available, or a lexical fallback when they are not. `export-context` writes the results as JSON or markdown for later analysis.

The default embedding backend is a hash-based vector, so the project runs without model downloads. If you install the ML extras, you can switch to sentence-transformer embeddings and zero-shot topic classification.

## Live Sync

Gmail sync uses the Gmail API with OAuth tokens and a checkpoint on the last seen internal timestamp. IMAP sync uses UID checkpoints and works with ordinary IMAP servers as well as Proton Mail Bridge setups. `run-sync` runs Gmail by default and adds IMAP when you pass `--imap`. Both sync paths use polling.

## Architecture Notes

- `src/tldr_crypto_finance/ingestion` handles MBOX, EML, Gmail, IMAP, dedupe, and checkpoints.
- `src/tldr_crypto_finance/parsing` handles HTML cleanup, section splitting, article splitting, links, and sponsor filtering.
- `src/tldr_crypto_finance/labeling` handles taxonomy loading, rules, zero-shot labeling hooks, embeddings, and entities.
- `src/tldr_crypto_finance/retrieval` handles SQL retrieval, similarity ranking, context export, and brief generation.
- `src/tldr_crypto_finance/db` contains the DuckDB schema and curated SQL views.

## Roadmap

- Improve sender-specific parsing profiles for newsletters that need custom section or block rules.
- Add stronger duplicate detection and clustering.
- Expand entity extraction with NLP backends.
- Add better review workflows for ambiguous sponsor cases and low-confidence labels.
- Add dashboards and notebook examples on top of the curated views.
