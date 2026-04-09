"""Historical backfill from MBOX mailbox exports."""

from __future__ import annotations

import logging
import mailbox
from pathlib import Path

import duckdb

from tldr_crypto_finance.db.duckdb import begin_run, finish_run, init_database
from tldr_crypto_finance.ingestion.common import insert_raw_message, parse_message_record

logger = logging.getLogger(__name__)


def ingest_mbox_file(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    *,
    sql_dir: Path,
) -> dict[str, int]:
    """Read an MBOX export and store normalized raw messages."""

    init_database(connection, sql_dir)
    run_id = begin_run(connection, "ingest_mbox", notes=str(path))
    stats = {"messages_seen": 0, "inserted": 0, "duplicates": 0, "errors": 0}
    mailbox_name = path.stem
    try:
        mbox = mailbox.mbox(path)
        for key, message in mbox.items():
            stats["messages_seen"] += 1
            try:
                record = parse_message_record(
                    message,
                    source_system="mbox",
                    source_mailbox=mailbox_name,
                    raw_path=f"{path}::{key}",
                    run_id=run_id,
                )
                inserted, _ = insert_raw_message(connection, record)
                stats["inserted" if inserted else "duplicates"] += 1
            except Exception:
                logger.exception("Failed to ingest MBOX message %s from %s", key, path)
                stats["errors"] += 1
        finish_run(connection, run_id, "success", notes=str(stats))
    except Exception:
        finish_run(connection, run_id, "failed", notes=str(stats))
        raise
    return stats
