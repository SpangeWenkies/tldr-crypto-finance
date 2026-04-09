"""High-level refresh orchestration over already ingested raw messages."""

from __future__ import annotations

from pathlib import Path

import duckdb

from tldr_crypto_finance.analytics.export_parquet import export_curated_parquet
from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.pipelines.enrich import label_articles, parse_issues
from tldr_crypto_finance.retrieval.semantic import build_article_embeddings


def run_full_refresh(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    sql_dir: Path,
    label_mode: str = "rules",
    force_parse: bool = False,
    force_label: bool = False,
    build_embeddings: bool = True,
    export_parquet: bool = True,
) -> dict:
    """Run parse, label, optional embeddings, and optional Parquet export."""

    parse_stats = parse_issues(connection, settings, sql_dir=sql_dir, force=force_parse)
    label_stats = label_articles(
        connection,
        settings,
        sql_dir=sql_dir,
        mode=label_mode,
        force=force_label,
    )
    results = {"parse": parse_stats, "label": label_stats}
    if build_embeddings:
        results["embeddings"] = build_article_embeddings(
            connection,
            backend="hash",
            model_name=settings.embeddings_model_name,
            force=force_label or force_parse,
        )
    if export_parquet:
        results["exported"] = [
            str(path) for path in export_curated_parquet(connection, settings.curated_data_dir)
        ]
    return results
