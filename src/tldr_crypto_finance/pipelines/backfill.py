"""Historical backfill orchestration from local mailbox exports."""

from __future__ import annotations

from pathlib import Path

import duckdb

from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.ingestion.eml_ingest import ingest_eml_directory
from tldr_crypto_finance.ingestion.mbox_ingest import ingest_mbox_file
from tldr_crypto_finance.pipelines.full_refresh import run_full_refresh


def run_backfill(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    sql_dir: Path,
    source_path: Path,
    source_type: str,
    label_mode: str = "rules",
    build_embeddings: bool = True,
    export_parquet: bool = True,
) -> dict:
    """Run a historical backfill from EML or MBOX through the full local pipeline."""

    if source_type == "eml":
        ingest_stats = ingest_eml_directory(connection, source_path, sql_dir=sql_dir)
    elif source_type == "mbox":
        ingest_stats = ingest_mbox_file(connection, source_path, sql_dir=sql_dir)
    else:
        msg = f"Unsupported backfill source type: {source_type}"
        raise ValueError(msg)

    refresh_stats = run_full_refresh(
        connection,
        settings,
        sql_dir=sql_dir,
        label_mode=label_mode,
        build_embeddings=build_embeddings,
        export_parquet=export_parquet,
    )
    return {"ingest": ingest_stats, **refresh_stats}
