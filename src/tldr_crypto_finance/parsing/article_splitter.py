"""Split newsletter sections into article-level blocks."""

from __future__ import annotations

import re

from tldr_crypto_finance.parsing.link_extractor import extract_links
from tldr_crypto_finance.utils.text import paragraphs_from_text, strip_urls

_SPONSOR_MARKERS = {"sponsored", "advertisement", "partner content", "presented by"}


def _looks_like_title(paragraph: str) -> bool:
    """Return True when a paragraph resembles a short standalone headline."""

    words = paragraph.split()
    if not words or len(words) > 14 or len(paragraph) > 120:
        return False
    if "http" in paragraph.lower() or paragraph.endswith((".", "!", "?")):
        return False
    if paragraph.lower() in _SPONSOR_MARKERS:
        return True
    return paragraph == paragraph.title() or paragraph == paragraph.upper()


def _should_start_new_block(current: list[str], paragraph: str, current_has_links: bool) -> bool:
    """Decide when a new paragraph should begin a fresh article block."""

    if not current:
        return False
    if paragraph.lower() in _SPONSOR_MARKERS:
        return True
    if _looks_like_title(paragraph) and len(" ".join(current).split()) >= 8:
        return True
    if current_has_links and "http" not in paragraph.lower():
        return True
    return False


def split_article_blocks(text: str, min_words: int = 25) -> list[str]:
    """Split a section body into article-sized text blocks."""

    paragraphs = paragraphs_from_text(text)
    if not paragraphs:
        return []

    blocks: list[str] = []
    current: list[str] = []
    current_has_links = False

    for paragraph in paragraphs:
        if _should_start_new_block(current, paragraph, current_has_links):
            blocks.append("\n\n".join(current))
            current = [paragraph]
            current_has_links = bool(extract_links(paragraph))
            continue
        current.append(paragraph)
        current_has_links = current_has_links or bool(extract_links(paragraph))

    if current:
        blocks.append("\n\n".join(current))

    if len(blocks) == 1 and len(re.findall(r"\n\n", blocks[0])) >= 4:
        paragraphs = paragraphs_from_text(blocks[0])
        paired = [
            "\n\n".join(paragraphs[index : index + 2])
            for index in range(0, len(paragraphs), 2)
            if paragraphs[index : index + 2]
        ]
        return paired

    compact_blocks: list[str] = []
    for block in blocks:
        if (
            compact_blocks
            and len(strip_urls(block).split()) < min_words
            and "http" not in block.lower()
        ):
            compact_blocks[-1] = f"{compact_blocks[-1]}\n\n{block}"
            continue
        compact_blocks.append(block)
    return compact_blocks


def extract_title(block_text: str) -> tuple[str | None, float]:
    """Extract a likely article title and return a heuristic confidence score."""

    paragraphs = paragraphs_from_text(block_text)
    if not paragraphs:
        return None, 0.0
    first = paragraphs[0]
    if _looks_like_title(first):
        return first, 0.9
    cleaned = strip_urls(first)
    if len(cleaned.split()) <= 16:
        return cleaned[:100] or None, 0.55
    sentence = re.split(r"[.!?]", cleaned, maxsplit=1)[0].strip()
    return sentence[:100] or None, 0.35
