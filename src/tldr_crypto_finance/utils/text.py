"""Text normalization helpers."""

from __future__ import annotations

import re
from typing import Final

_WHITESPACE_RE = re.compile(r"[ \t]+")
_MULTILINE_RE = re.compile(r"\n{3,}")
_TOKEN_RE = re.compile(r"[A-Za-z0-9_.$%-]+")
_URL_RE: Final[re.Pattern[str]] = re.compile(r"https?://[^\s)>\"']+")


def normalize_line_endings(text: str) -> str:
    """Normalize Windows and classic Mac line endings to Unix newlines."""

    return text.replace("\r\n", "\n").replace("\r", "\n")


def collapse_whitespace(text: str) -> str:
    """Collapse repeated spaces and blank lines while preserving paragraphs."""

    compact = _WHITESPACE_RE.sub(" ", normalize_line_endings(text))
    return _MULTILINE_RE.sub("\n\n", compact).strip()


def simple_tokens(text: str) -> list[str]:
    """Extract lowercase word tokens for lightweight retrieval and matching."""

    return [token.lower() for token in _TOKEN_RE.findall(text)]


def paragraphs_from_text(text: str) -> list[str]:
    """Split normalized text into non-empty paragraphs."""

    normalized = collapse_whitespace(text)
    paragraphs = [paragraph.strip() for paragraph in normalized.split("\n\n") if paragraph.strip()]
    if len(paragraphs) <= 1:
        lines = [line.strip() for line in normalized.splitlines() if line.strip()]
        return lines if len(lines) > 1 else paragraphs

    expanded: list[str] = []
    for paragraph in paragraphs:
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        if len(lines) > 1 and (
            any("http" in line.lower() for line in lines)
            or any(len(line.split()) <= 6 for line in lines)
        ):
            expanded.extend(lines)
        else:
            expanded.append(paragraph)
    return expanded


def strip_urls(text: str) -> str:
    """Remove inline URLs from text while leaving surrounding prose intact."""

    cleaned = _URL_RE.sub("", text)
    return collapse_whitespace(cleaned)


def slugify(text: str) -> str:
    """Create a filesystem and SQL friendly slug from a short label."""

    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "unknown"
