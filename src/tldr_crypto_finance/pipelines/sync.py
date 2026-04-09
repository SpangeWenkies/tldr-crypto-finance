"""High-level sync orchestration for live mailbox sources."""

from __future__ import annotations

from pathlib import Path

import duckdb

from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.ingestion.gmail_ingest import sync_gmail
from tldr_crypto_finance.ingestion.imap_ingest import sync_imap


def run_sync(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    sql_dir: Path,
    gmail: bool = True,
    imap: bool = False,
) -> dict[str, dict[str, int]]:
    """Run the selected live sync sources and return per-source stats."""

    if not gmail and not imap:
        raise RuntimeError("No sync sources selected. Enable Gmail or IMAP.")

    results: dict[str, dict[str, int]] = {}
    if gmail:
        results["gmail"] = sync_gmail(connection, settings, sql_dir=sql_dir)
    if imap:
        results["imap"] = sync_imap(connection, settings, sql_dir=sql_dir)
    return results
