"""URL canonicalization helpers."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

TRACKING_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "mc_cid",
    "mc_eid",
}


def canonicalize_url(url: str) -> str:
    """Strip common tracking parameters and normalize a URL."""

    parsed = urlparse(url.strip())
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key not in TRACKING_KEYS
    ]
    normalized = parsed._replace(query=urlencode(query), fragment="")
    return urlunparse(normalized)


def extract_domain(url: str) -> str:
    """Return the lowercase network location for a URL."""

    return urlparse(url).netloc.lower()
