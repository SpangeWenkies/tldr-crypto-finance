"""Hash helpers for stable record identifiers."""

from __future__ import annotations

import hashlib


def stable_hash(*values: object) -> str:
    """Build a SHA256 hash from a sequence of values."""

    normalized = "||".join("" if value is None else str(value) for value in values)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def text_hash(text: str) -> str:
    """Hash a single text value after trimming leading and trailing whitespace."""

    return stable_hash(text.strip())
