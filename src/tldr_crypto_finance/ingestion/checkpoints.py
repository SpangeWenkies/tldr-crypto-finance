"""Checkpoint helpers for sync-oriented ingestion modes."""

from __future__ import annotations

import duckdb

from tldr_crypto_finance.db.duckdb import get_checkpoint, set_checkpoint


def read_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    sync_source: str,
    checkpoint_key: str,
) -> str | None:
    """Load a stored checkpoint value for an incremental source."""

    return get_checkpoint(connection, sync_source, checkpoint_key)


def write_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    sync_source: str,
    checkpoint_key: str,
    checkpoint_value: str,
) -> None:
    """Persist a checkpoint after a successful sync step."""

    set_checkpoint(connection, sync_source, checkpoint_key, checkpoint_value)
