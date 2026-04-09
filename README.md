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

- The commands below assume macOS with `zsh`.
- Do these steps first, in this order.
- You only need to fill the Gmail or IMAP values in `.env` if you choose a live sync flow later.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -e ".[dev]"
cp .env.example .env
python3 -m tldr_crypto_finance.cli init-db
```

## Choose An Input Source

### Option 1: `.eml` directory

An `.eml` export is a directory of raw email files, usually one file per message. Mail, Gmail, Outlook, and other mail clients can export messages in this format.

If you already have an `.eml` directory, this is the shortest path. `run-backfill` ingests the files and runs the downstream parse, labeling, embedding, and Parquet export steps.

```bash
python3 -m tldr_crypto_finance.cli run-backfill /path/to/eml_directory --source eml
```

### Option 2: `.mbox` archive

An `.mbox` file is a mailbox export stored as one archive file containing many messages.

If you have an `.mbox` archive instead of `.eml` files, use:

```bash
python3 -m tldr_crypto_finance.cli run-backfill /path/to/archive.mbox --source mbox
```

### Option 3: Gmail sync

Use this when the messages are still in Gmail and you want to pull new mail into the local database.

Google links:

- Create a project: [Google Cloud Console](https://console.cloud.google.com/cloud-resource-manager)
- Project docs: [Create and manage projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
- Enable Gmail API: [Gmail API in Cloud Console](https://console.cloud.google.com/apis/library/gmail.googleapis.com)
- Gmail Python quickstart: [Gmail API Python quickstart](https://developers.google.com/workspace/gmail/api/quickstart/python)
- OAuth consent docs: [Configure OAuth consent](https://developers.google.com/workspace/guides/configure-oauth-consent)
- OAuth branding page: [Auth Branding](https://console.cloud.google.com/auth/branding)
- OAuth client page: [Auth Clients](https://console.cloud.google.com/auth/clients)

1. Create or choose a Google Cloud project.
2. Enable the Gmail API for that project.
3. Configure the OAuth consent screen.
4. If you are using a personal Gmail account, choose `External` and add yourself as a test user. `Internal` is for Google Workspace organization-only apps.
5. Create an OAuth client ID with application type `Desktop app`.
6. Download the OAuth client JSON file.
7. Put that file at `secrets/gmail_credentials.json`, or change `TLDR_CRYPTO_FINANCE_GMAIL_CREDENTIALS_PATH` in `.env` to point somewhere else.
8. Leave `TLDR_CRYPTO_FINANCE_GMAIL_TOKEN_PATH` as `secrets/gmail_token.json` unless you want a different location. This file is created automatically after the first successful login.
9. Set the Gmail values in `.env`:

```dotenv
TLDR_CRYPTO_FINANCE_GMAIL_CREDENTIALS_PATH=secrets/gmail_credentials.json
TLDR_CRYPTO_FINANCE_GMAIL_TOKEN_PATH=secrets/gmail_token.json
TLDR_CRYPTO_FINANCE_GMAIL_QUERY_FILTER=label:newsletters newer_than:30d
```

`TLDR_CRYPTO_FINANCE_GMAIL_CREDENTIALS_PATH` must point to the downloaded OAuth client JSON. `TLDR_CRYPTO_FINANCE_GMAIL_TOKEN_PATH` does not need to exist yet.

Then run:

```bash
python3 -m tldr_crypto_finance.cli sync-gmail
```

The first run opens a browser for OAuth, asks for Gmail read-only access, and then writes the token file. Later runs reuse that token.

### Option 4: IMAP sync

Use this when the messages live in another mailbox that supports IMAP.

Set these values in `.env`:

```dotenv
TLDR_CRYPTO_FINANCE_IMAP_HOST=
TLDR_CRYPTO_FINANCE_IMAP_PORT=993
TLDR_CRYPTO_FINANCE_IMAP_USERNAME=
TLDR_CRYPTO_FINANCE_IMAP_PASSWORD=
TLDR_CRYPTO_FINANCE_IMAP_FOLDER=INBOX
```

Then run:

```bash
python3 -m tldr_crypto_finance.cli run-sync --no-gmail --imap
```

## Build Derived Tables

If you used `run-backfill`, this section is already done.

If you used `sync-gmail` or `run-sync`, those commands only add new raw messages. Run the steps below afterward to refresh the parsed and labeled tables.

If you want raw ingestion without `run-backfill`, use one of these first:

```bash
python3 -m tldr_crypto_finance.cli ingest-eml /path/to/eml_directory
python3 -m tldr_crypto_finance.cli ingest-mbox /path/to/archive.mbox
```

Then run:

```bash
python3 -m tldr_crypto_finance.cli parse-issues
python3 -m tldr_crypto_finance.cli label-articles --force
python3 -m tldr_crypto_finance.cli build-embeddings --force
python3 -m tldr_crypto_finance.cli export-parquet
```

## Query And Review

Query the resulting article store:

```bash
python3 -m tldr_crypto_finance.cli query-articles --topic crypto_markets --output markdown
```

Generate a short brief:

```bash
python3 -m tldr_crypto_finance.cli risk-brief stablecoin --topic crypto_markets
```

Search by similarity:

```bash
python3 -m tldr_crypto_finance.cli search-similar "stablecoin liquidity reserve backing" --output markdown
```

Export a JSON or markdown context bundle:

```bash
python3 -m tldr_crypto_finance.cli export-context --topic crypto_markets --output json
```

Open the notebook:

```bash
jupyter notebook notebooks/01_exploration.ipynb
```

If something looks wrong, inspect these first:

- `raw_messages` for ingestion problems
- `newsletter_issues`, `sections`, and `article_blocks` for parse quality
- `article_labels` and `manual_review_queue` for labeling gaps
- `runs` for pipeline run history
- `v_parse_quality_by_newsletter`, `v_low_confidence_labels`, and `v_duplicates` for cleanup work

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
