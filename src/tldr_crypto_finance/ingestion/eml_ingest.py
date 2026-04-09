"""Historical backfill from standalone EML files."""

from __future__ import annotations

import logging
from email import policy
from email.parser import BytesParser
from pathlib import Path

import duckdb

from tldr_crypto_finance.db.duckdb import begin_run, finish_run, init_database
from tldr_crypto_finance.ingestion.common import insert_raw_message, parse_message_record

logger = logging.getLogger(__name__)


def _parse_eml_file(path: Path):
    """Parse a single EML file with a permissive policy."""

    with path.open("rb") as handle:
        return BytesParser(policy=policy.default).parse(handle)


def ingest_eml_directory(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    sql_dir: Path,
) -> dict[str, int]:
    """Walk a directory tree of EML files and ingest them into raw storage."""

    init_database(connection, sql_dir)
    run_id = begin_run(connection, "ingest_eml", notes=str(path))
    stats = {"files_seen": 0, "inserted": 0, "duplicates": 0, "errors": 0}
    try:
        for eml_path in sorted(path.rglob("*.eml")):
            stats["files_seen"] += 1
            try:
                message = _parse_eml_file(eml_path)
                record = parse_message_record(
                    message,
                    source_system="eml",
                    source_mailbox=eml_path.parent.name,
                    raw_path=str(eml_path),
                    run_id=run_id,
                )
                inserted, _ = insert_raw_message(connection, record)
                stats["inserted" if inserted else "duplicates"] += 1
            except Exception:
                logger.exception("Failed to ingest EML file %s", eml_path)
                stats["errors"] += 1
        finish_run(connection, run_id, "success", notes=str(stats))
    except Exception:
        finish_run(connection, run_id, "failed", notes=str(stats))
        raise
    return stats
