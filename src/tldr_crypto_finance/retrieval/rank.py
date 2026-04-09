"""Ranking helpers for retrieval."""

from __future__ import annotations

import math

from tldr_crypto_finance.utils.text import simple_tokens


def cosine_similarity(left: list[float], right: list[float]) -> float:
    """Compute cosine similarity for two already aligned vectors."""

    if not left or not right:
        return 0.0
    return sum(a * b for a, b in zip(left, right, strict=False))


def lexical_similarity(query_text: str, candidate_text: str) -> float:
    """Compute a lightweight lexical similarity score from token overlap."""

    left = set(simple_tokens(query_text))
    right = set(simple_tokens(candidate_text))
    if not left or not right:
        return 0.0
    intersection = len(left.intersection(right))
    return intersection / math.sqrt(len(left) * len(right))
