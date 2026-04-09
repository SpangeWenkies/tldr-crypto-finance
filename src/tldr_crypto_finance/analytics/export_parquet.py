"""Parquet export utilities."""

from __future__ import annotations

from pathlib import Path

import duckdb

DEFAULT_EXPORTS = {
    "articles.parquet": "SELECT * FROM v_non_sponsored_articles",
    "labels.parquet": "SELECT * FROM article_labels",
    "review_queue.parquet": "SELECT * FROM manual_review_queue",
    "latest_articles.parquet": "SELECT * FROM v_latest_articles",
    "watchlist_hits.parquet": "SELECT * FROM v_watchlist_hits",
    "asset_risk_slices.parquet": "SELECT * FROM v_asset_risk_slices",
    "duplicates.parquet": "SELECT * FROM v_duplicates",
}


def export_curated_parquet(connection: duckdb.DuckDBPyConnection, output_dir: Path) -> list[Path]:
    """Export curated tables and views to local Parquet files."""

    output_dir.mkdir(parents=True, exist_ok=True)
    exported: list[Path] = []
    for filename, query in DEFAULT_EXPORTS.items():
        path = output_dir / filename
        connection.execute(f"COPY ({query}) TO '{path.as_posix()}' (FORMAT PARQUET)")
        exported.append(path)
    return exported
