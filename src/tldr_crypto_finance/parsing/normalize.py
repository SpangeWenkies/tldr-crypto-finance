"""Text normalization for newsletter parsing."""

from __future__ import annotations

import html

from tldr_crypto_finance.utils.text import collapse_whitespace


def normalize_newsletter_text(text: str) -> str:
    """Normalize newsletter body text while preserving paragraph boundaries."""

    return collapse_whitespace(html.unescape(text))
