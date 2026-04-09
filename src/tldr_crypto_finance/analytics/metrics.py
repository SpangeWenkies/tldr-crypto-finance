"""Operational metrics over ingestion and parsing output."""

from __future__ import annotations

from typing import Any

import duckdb


def collect_metrics(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Collect core ingestion and parsing metrics from the database."""

    scalar_queries = {
        "messages_ingested": "SELECT count(*) FROM raw_messages",
        "issues_parsed": "SELECT count(*) FROM newsletter_issues",
        "article_blocks": "SELECT count(*) FROM article_blocks",
        "sponsored_blocks": "SELECT count(*) FROM article_blocks WHERE keep_for_analysis = FALSE",
        "review_queue": "SELECT count(*) FROM manual_review_queue WHERE current_status = 'open'",
        "unlabeled_articles": """
            SELECT count(*)
            FROM article_blocks ab
            LEFT JOIN article_labels al ON al.article_id = ab.article_id
            WHERE al.article_id IS NULL
              AND ab.keep_for_analysis = TRUE
        """,
        "gmail_checkpoint_rows": (
            "SELECT count(*) FROM sync_checkpoints WHERE sync_source = 'gmail'"
        ),
        "imap_checkpoint_rows": (
            "SELECT count(*) FROM sync_checkpoints WHERE sync_source = 'imap'"
        ),
        "duplicate_article_pairs": "SELECT count(*) FROM v_duplicates",
        "low_confidence_labels": "SELECT count(*) FROM v_low_confidence_labels",
        "watchlist_hits": "SELECT count(*) FROM v_watchlist_hits",
    }
    metrics = {
        key: connection.execute(query).fetchone()[0]
        for key, query in scalar_queries.items()
    }
    messages_ingested = int(metrics["messages_ingested"])
    metrics["parse_coverage_ratio"] = (
        round(float(metrics["issues_parsed"]) / messages_ingested, 4) if messages_ingested else 0.0
    )
    metrics["ingestion_by_source"] = {
        source: count
        for source, count in connection.execute(
            """
            SELECT source_system, count(*)
            FROM raw_messages
            GROUP BY 1
            ORDER BY 2 DESC
            """
        ).fetchall()
    }
    metrics["open_reviews_by_reason"] = {
        reason: count
        for reason, count in connection.execute(
            """
            SELECT review_reason, count(*)
            FROM manual_review_queue
            WHERE current_status = 'open'
            GROUP BY 1
            ORDER BY 2 DESC
            """
        ).fetchall()
    }
    return metrics
