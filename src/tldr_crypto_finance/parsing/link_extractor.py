"""Link extraction from newsletter article blocks."""

from __future__ import annotations

import re

from tldr_crypto_finance.utils.urls import canonicalize_url, extract_domain

URL_RE = re.compile(r"https?://[^\s)>\"']+")


def extract_links(text: str) -> list[dict[str, str | int | None]]:
    """Extract and deduplicate links from a block of newsletter text."""

    links: list[dict[str, str | int | None]] = []
    seen: set[str] = set()
    for order, match in enumerate(URL_RE.finditer(text), start=1):
        original_url = match.group(0).rstrip(".,)")
        canonical_url = canonicalize_url(original_url)
        if canonical_url in seen:
            continue
        seen.add(canonical_url)
        line = next(
            (
                candidate.strip()
                for candidate in text.splitlines()
                if original_url in candidate or canonical_url in candidate
            ),
            "",
        )
        link_text = line.replace(original_url, "").replace(canonical_url, "").strip(" :-") or None
        links.append(
            {
                "original_url": original_url,
                "canonical_url": canonical_url,
                "domain": extract_domain(canonical_url),
                "link_text": link_text,
                "link_order": order,
            }
        )
    return links
