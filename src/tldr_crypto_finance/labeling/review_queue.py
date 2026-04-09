"""Helpers for manual review queue maintenance."""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb

from tldr_crypto_finance.utils.hashing import stable_hash


def clear_label_reviews(connection: duckdb.DuckDBPyConnection, article_id: str) -> None:
    """Remove existing label-related review items before relabeling an article."""

    connection.execute(
        """
        DELETE FROM manual_review_queue
        WHERE article_id = ? AND review_reason LIKE 'label:%'
        """,
        [article_id],
    )


def enqueue_label_review(
    connection: duckdb.DuckDBPyConnection,
    article_id: str,
    review_reason: str,
) -> None:
    """Add a label-related review item if it does not already exist."""

    review_id = stable_hash(article_id, review_reason)
    connection.execute(
        """
        INSERT INTO manual_review_queue (
            review_id,
            article_id,
            review_reason,
            current_status,
            created_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (review_id) DO NOTHING
        """,
        [review_id, article_id, review_reason, "open", datetime.now(tz=UTC)],
    )
