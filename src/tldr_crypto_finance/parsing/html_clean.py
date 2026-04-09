"""HTML cleanup for newsletter bodies."""

from __future__ import annotations

from bs4 import BeautifulSoup

from tldr_crypto_finance.parsing.normalize import normalize_newsletter_text


def html_to_text(html_body: str) -> str:
    """Convert newsletter HTML into paragraph-oriented plain text."""

    soup = BeautifulSoup(html_body, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    for tag in soup.find_all(["br"]):
        tag.replace_with("\n")
    text = soup.get_text("\n")
    return normalize_newsletter_text(text)


def pick_preferred_body(text_body: str, html_body: str) -> str:
    """Choose the richer newsletter body, preferring cleaned HTML when useful."""

    normalized_text = normalize_newsletter_text(text_body)
    normalized_html = html_to_text(html_body) if html_body else ""
    if normalized_html and len(normalized_html) > max(len(normalized_text), 80):
        return normalized_html
    return normalized_text or normalized_html
