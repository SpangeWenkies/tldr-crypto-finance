"""Split newsletter text into configurable sections."""

from __future__ import annotations

import re

from tldr_crypto_finance.models.records import SectionCandidate
from tldr_crypto_finance.utils.text import paragraphs_from_text


def _looks_like_section_heading(paragraph: str, patterns: list[str]) -> bool:
    """Return True when a paragraph resembles a section header."""

    for pattern in patterns:
        if re.match(pattern, paragraph):
            return True
    words = paragraph.split()
    if len(words) > 6 or len(paragraph) > 60:
        return False
    if "http" in paragraph.lower() or paragraph.endswith((".", "!", "?", ":")):
        return False
    alpha_words = re.findall(r"[A-Za-z]+", paragraph)
    return paragraph == paragraph.upper() or all(word[0].isupper() for word in alpha_words)


def split_sections(text: str, section_patterns: list[str]) -> list[SectionCandidate]:
    """Split a newsletter body into named sections using heuristics plus config."""

    paragraphs = paragraphs_from_text(text)
    if not paragraphs:
        return []

    sections: list[SectionCandidate] = []
    current_name = "General"
    current_paragraphs: list[str] = []

    for paragraph in paragraphs:
        if _looks_like_section_heading(paragraph, section_patterns):
            if current_paragraphs:
                sections.append(
                    SectionCandidate(
                        name=current_name,
                        order=len(sections) + 1,
                        raw_text="\n\n".join(current_paragraphs),
                    )
                )
                current_paragraphs = []
            current_name = paragraph
            continue
        current_paragraphs.append(paragraph)

    if current_paragraphs:
        sections.append(
            SectionCandidate(
                name=current_name,
                order=len(sections) + 1,
                raw_text="\n\n".join(current_paragraphs),
            )
        )
    return sections or [SectionCandidate(name="General", order=1, raw_text=text)]
