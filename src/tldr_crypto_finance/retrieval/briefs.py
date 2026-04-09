"""Brief generation for analyst and agent workflows."""

from __future__ import annotations

from collections import Counter

from tldr_crypto_finance.retrieval.query import query_articles
from tldr_crypto_finance.utils.text import collapse_whitespace


def build_risk_brief(records: list[dict], subject: str) -> str:
    """Render a short markdown risk brief from retrieved article records."""

    if not records:
        return f"## Risk Brief: {subject}\n\n- No matching non-sponsored articles were found."

    domains = Counter(record.get("primary_domain") or "unknown" for record in records)
    lines = [f"## Risk Brief: {subject}", ""]
    lines.append(f"- Articles reviewed: {len(records)}")
    lines.append(
        "- Sources: "
        + ", ".join(f"{domain} ({count})" for domain, count in domains.most_common(3))
    )
    lines.append("- Key items:")
    for record in records[:5]:
        title = record.get("extracted_title") or "(untitled)"
        summary = collapse_whitespace(
            str(record.get("clean_summary_text") or "").replace("\n", " ")
        )[:180]
        url = record.get("canonical_url") or ""
        lines.append(f"- {title}: {summary} {url}".rstrip())
    return "\n".join(lines)


def risk_brief(
    connection,
    subject: str,
    *,
    days: int = 14,
    topic: str | None = None,
    limit: int = 5,
) -> str:
    """Fetch recent articles and render a compact risk brief."""

    def _subject_matches(records: list[dict]) -> list[dict]:
        return [
            record
            for record in records
            if subject.lower() in str(record.get("clean_summary_text") or "").lower()
            or subject.lower() in str(record.get("extracted_title") or "").lower()
            or subject.lower() in str(record.get("topic") or "").lower()
            or subject.lower() in str(record.get("subtopic") or "").lower()
        ]

    records = query_articles(connection, days=days, topic=topic, limit=limit)
    filtered = _subject_matches(records)
    if filtered:
        return build_risk_brief(filtered, subject)

    if days is not None:
        all_time_records = query_articles(connection, days=None, topic=topic, limit=limit)
        all_time_filtered = _subject_matches(all_time_records)
        if all_time_filtered:
            return build_risk_brief(all_time_filtered, subject)
        return build_risk_brief(all_time_records, subject)

    return build_risk_brief(records[:limit], subject)
