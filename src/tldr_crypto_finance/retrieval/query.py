"""Structured SQL retrieval over labeled articles."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import duckdb

from tldr_crypto_finance.utils.text import collapse_whitespace


def query_articles(
    connection: duckdb.DuckDBPyConnection,
    *,
    days: int | None = None,
    topic: str | None = None,
    sender: str | None = None,
    domain: str | None = None,
    asset_class: str | None = None,
    risk_type: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query non-sponsored articles with optional structured filters."""

    clauses = ["ab.keep_for_analysis = TRUE"]
    params: list[Any] = []
    if days is not None:
        cutoff = datetime.now(tz=UTC) - timedelta(days=days)
        clauses.append("rm.sent_at >= ?")
        params.append(cutoff)
    if topic:
        clauses.append("al.topic = ?")
        params.append(topic)
    if sender:
        clauses.append("lower(coalesce(rm.sender_email, '')) LIKE ?")
        params.append(f"%{sender.lower()}%")
    if domain:
        clauses.append("ab.primary_domain = ?")
        params.append(domain.lower())
    if asset_class:
        clauses.append("al.asset_class = ?")
        params.append(asset_class)
    if risk_type:
        clauses.append("al.risk_type = ?")
        params.append(risk_type)

    query = f"""
        SELECT
            ab.article_id,
            ab.extracted_title,
            ab.clean_summary_text,
            ab.canonical_url,
            ab.primary_domain,
            ab.parse_confidence,
            al.topic,
            al.subtopic,
            al.asset_class,
            al.risk_type,
            al.region,
            al.sentiment_tone,
            al.urgency,
            al.confidence AS label_confidence,
            rm.sent_at,
            rm.sender_email,
            ni.newsletter_name,
            s.section_name
        FROM article_blocks ab
        JOIN newsletter_issues ni ON ni.issue_id = ab.issue_id
        JOIN raw_messages rm ON rm.internal_message_pk = ni.internal_message_pk
        LEFT JOIN sections s ON s.section_id = ab.section_id
        LEFT JOIN article_labels al ON al.article_id = ab.article_id
        WHERE {' AND '.join(clauses)}
        ORDER BY rm.sent_at DESC NULLS LAST, ab.block_order ASC
        LIMIT {int(limit)}
    """
    cursor = connection.execute(query, params)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def context_bundle(records: list[dict[str, Any]], output_format: str = "json") -> str:
    """Render records as JSON or compact markdown for downstream agents."""

    if output_format == "markdown":
        lines = []
        for record in records:
            title = record.get("extracted_title") or "(untitled)"
            summary = collapse_whitespace(
                str(record.get("clean_summary_text") or "").replace("\n", " ")
            )
            url = record.get("canonical_url") or ""
            sender = record.get("newsletter_name") or record.get("sender_email") or "unknown"
            lines.append(f"- {title} | {sender} | {summary[:180]} | {url}")
        return "\n".join(lines)
    return json.dumps(records, indent=2, default=str)
