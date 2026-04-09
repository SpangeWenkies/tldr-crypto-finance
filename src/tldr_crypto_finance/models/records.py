"""Typed record models for ingestion, parsing, and retrieval."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(slots=True)
class RawMessageRecord:
    """Normalized email message stored before issue parsing."""

    internal_message_pk: str
    source_system: str
    source_message_id: str | None
    source_mailbox: str | None
    sender_name: str | None
    sender_email: str | None
    subject: str | None
    sent_at: datetime | None
    received_at: datetime | None
    ingested_at: datetime
    text_body: str
    html_body: str
    body_hash: str
    raw_path: str | None
    run_id: str


@dataclass(slots=True)
class SectionCandidate:
    """Candidate newsletter section built during parsing."""

    name: str
    order: int
    raw_text: str


@dataclass(slots=True)
class ArticleCandidate:
    """Candidate article block produced by the parser."""

    issue_id: str
    section_id: str | None
    block_order: int
    raw_block_text: str
    clean_summary_text: str
    extracted_title: str | None
    title_confidence: float
    canonical_url: str | None
    primary_domain: str | None
    is_sponsored_rule: bool
    is_sponsored_ml: bool | None
    sponsor_confidence: float
    keep_for_analysis: bool
    parse_confidence: float
    links: list[dict[str, str | int | None]] = field(default_factory=list)


@dataclass(slots=True)
class LabelRecord:
    """Assigned article labels from rules or optional models."""

    article_id: str
    topic: str | None
    subtopic: str | None
    asset_class: str | None
    risk_type: str | None
    region: str | None
    sentiment_tone: str | None
    urgency: str | None
    label_source: str
    confidence: float
