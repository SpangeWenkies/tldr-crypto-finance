"""Embedding-backed and fallback similarity search."""

from __future__ import annotations

from typing import Any

import duckdb

from tldr_crypto_finance.labeling.embeddings import (
    build_embedding,
    dumps_vector,
    loads_vector,
)
from tldr_crypto_finance.retrieval.query import query_articles
from tldr_crypto_finance.retrieval.rank import cosine_similarity, lexical_similarity


def build_article_embeddings(
    connection: duckdb.DuckDBPyConnection,
    *,
    backend: str,
    model_name: str,
    force: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Build embeddings for analysis-ready articles and store them in DuckDB."""

    query = """
        SELECT article_id, clean_summary_text
        FROM article_blocks
        WHERE keep_for_analysis = TRUE
    """
    if not force:
        query += """
            AND article_id NOT IN (
                SELECT article_id FROM article_embeddings WHERE model_name = ?
            )
        """
        params: list[Any] = [model_name if backend == "sentence-transformer" else "hash-128-v1"]
    else:
        params = []
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    rows = connection.execute(query, params).fetchall()
    model_key = model_name if backend == "sentence-transformer" else "hash-128-v1"
    if force:
        connection.execute("DELETE FROM article_embeddings WHERE model_name = ?", [model_key])

    built = 0
    for article_id, text in rows:
        stored_model_name, vector = build_embedding(text or "", backend, model_name)
        connection.execute(
            """
            INSERT INTO article_embeddings (article_id, model_name, vector_json, created_at)
            VALUES (?, ?, ?, current_timestamp)
            ON CONFLICT (article_id, model_name) DO UPDATE
            SET vector_json = excluded.vector_json,
                created_at = excluded.created_at
            """,
            [article_id, stored_model_name, dumps_vector(vector)],
        )
        built += 1
    return {"articles_embedded": built}


def search_similar_articles(
    connection: duckdb.DuckDBPyConnection,
    query_text: str,
    *,
    backend: str = "hash",
    model_name: str = "",
    days: int | None = None,
    topic: str | None = None,
    sender: str | None = None,
    domain: str | None = None,
    asset_class: str | None = None,
    risk_type: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search for similar articles using embeddings when available, else lexical scoring."""

    candidates = query_articles(
        connection,
        days=days,
        topic=topic,
        sender=sender,
        domain=domain,
        asset_class=asset_class,
        risk_type=risk_type,
        limit=max(limit * 5, 25),
    )
    if not candidates:
        return []

    model_key = model_name if backend == "sentence-transformer" else "hash-128-v1"
    embedding_rows = connection.execute(
        """
        SELECT article_id, vector_json
        FROM article_embeddings
        WHERE model_name = ?
        """,
        [model_key],
    ).fetchall()
    embedding_map = {article_id: loads_vector(payload) for article_id, payload in embedding_rows}

    if embedding_map:
        _, query_vector = build_embedding(query_text, backend, model_name)
        for record in candidates:
            article_id = str(record["article_id"])
            record["score"] = cosine_similarity(query_vector, embedding_map.get(article_id, []))
    else:
        for record in candidates:
            record["score"] = lexical_similarity(
                query_text,
                str(record.get("clean_summary_text") or ""),
            )

    ranked = sorted(candidates, key=lambda item: float(item.get("score", 0.0)), reverse=True)
    return ranked[:limit]
