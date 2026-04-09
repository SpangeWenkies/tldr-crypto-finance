"""DuckDB connection and lifecycle helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb

from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.utils.hashing import stable_hash
from tldr_crypto_finance.utils.time import utc_now


def _load_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@contextmanager
def connect(settings: Settings) -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield a DuckDB connection rooted at the configured local database."""

    connection = duckdb.connect(str(settings.database_path))
    try:
        yield connection
    finally:
        connection.close()


@contextmanager
def connect_read_only(settings: Settings) -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield a read-only DuckDB connection for query-style commands."""

    connection = duckdb.connect(str(settings.database_path), read_only=True)
    try:
        yield connection
    finally:
        connection.close()


def init_database(connection: duckdb.DuckDBPyConnection, sql_dir: Path) -> None:
    """Create the database schema and curated SQL views."""

    connection.execute(_load_sql(sql_dir / "schema.sql"))
    connection.execute(_load_sql(sql_dir / "views.sql"))


def begin_run(
    connection: duckdb.DuckDBPyConnection,
    pipeline_step: str,
    notes: str | None = None,
) -> str:
    """Create a run record and return its identifier."""

    now = utc_now()
    run_id = stable_hash(pipeline_step, now.isoformat())
    connection.execute(
        """
        INSERT INTO runs (run_id, pipeline_step, started_at, status, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        [run_id, pipeline_step, now, "running", notes],
    )
    return run_id


def finish_run(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    status: str,
    notes: str | None = None,
) -> None:
    """Mark a run as finished with status and optional notes."""

    connection.execute(
        """
        UPDATE runs
        SET finished_at = ?, status = ?, notes = coalesce(?, notes)
        WHERE run_id = ?
        """,
        [utc_now(), status, notes, run_id],
    )


def get_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    sync_source: str,
    checkpoint_key: str,
) -> str | None:
    """Fetch a previously stored checkpoint value."""

    row = connection.execute(
        """
        SELECT checkpoint_value
        FROM sync_checkpoints
        WHERE sync_source = ? AND checkpoint_key = ?
        """,
        [sync_source, checkpoint_key],
    ).fetchone()
    return None if row is None else str(row[0])


def set_checkpoint(
    connection: duckdb.DuckDBPyConnection,
    sync_source: str,
    checkpoint_key: str,
    checkpoint_value: str,
) -> None:
    """Persist an ingestion checkpoint for later incremental sync runs."""

    connection.execute(
        """
        INSERT INTO sync_checkpoints (sync_source, checkpoint_key, checkpoint_value, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (sync_source, checkpoint_key) DO UPDATE
        SET checkpoint_value = excluded.checkpoint_value,
            updated_at = excluded.updated_at
        """,
        [sync_source, checkpoint_key, checkpoint_value, utc_now()],
    )
