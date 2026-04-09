"""Rule-based sponsor detection for article blocks."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(slots=True)
class SponsorDecision:
    """Result of sponsor filtering for one article block."""

    is_sponsored: bool
    confidence: float
    reason: str
    ambiguous: bool


def _contains_phrase(text: str, phrase: str) -> bool:
    """Match sponsor phrases with word boundaries to avoid substring false positives."""

    escaped = re.escape(phrase)
    pattern = rf"(?<!\w){escaped}(?!\w)"
    return re.search(pattern, text) is not None


def detect_sponsorship(
    block_text: str,
    links: list[dict[str, str | int | None]],
    rules: dict,
) -> SponsorDecision:
    """Detect likely sponsored blocks from text and known sponsor domains."""

    lowered = block_text.lower()
    keyword_hits = [item for item in rules.get("keywords", []) if _contains_phrase(lowered, item)]
    affiliate_hits = [
        item for item in rules.get("affiliate_phrases", []) if _contains_phrase(lowered, item)
    ]
    sponsor_domains = set(rules.get("sponsor_domains", []))
    ambiguous_hits = [
        item for item in rules.get("ambiguous_keywords", []) if _contains_phrase(lowered, item)
    ]
    link_domains = {str(link.get("domain") or "") for link in links}
    matching_domains = sponsor_domains.intersection(link_domains)

    score = 0.0
    if keyword_hits:
        score += 0.7
    if affiliate_hits:
        score += 0.2
    if matching_domains:
        score += 0.4
    ambiguous = bool(ambiguous_hits) and not keyword_hits and not matching_domains

    if keyword_hits or matching_domains or (affiliate_hits and ambiguous_hits):
        return SponsorDecision(
            is_sponsored=True,
            confidence=min(score, 1.0),
            reason="explicit_keyword" if keyword_hits else "sponsor_domain",
            ambiguous=False,
        )
    if ambiguous:
        return SponsorDecision(
            is_sponsored=False,
            confidence=0.4,
            reason="ambiguous_keyword",
            ambiguous=True,
        )
    return SponsorDecision(
        is_sponsored=False,
        confidence=min(score, 0.3),
        reason="editorial",
        ambiguous=False,
    )
