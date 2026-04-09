"""Lightweight entity extraction for finance and risk text."""

from __future__ import annotations

import re

ENTITY_PATTERNS = {
    "ticker": re.compile(r"(?<![A-Z0-9])\$?[A-Z]{2,5}(?![A-Z0-9])"),
}

COMPANIES = {"circle", "coinbase", "binance", "kraken", "apple", "microsoft"}
EXCHANGES = {"nasdaq", "nyse", "cme", "coinbase", "binance"}
REGULATORS = {"sec", "cftc", "ecb", "fed", "esma", "ofac"}
COUNTRIES = {"united states", "europe", "china", "japan", "uk"}
CHAINS = {"ethereum", "solana", "bitcoin", "arbitrum", "base"}
MACRO_TERMS = {"inflation", "rates", "liquidity", "yield curve", "deposit beta"}


def extract_entities(text: str) -> list[dict[str, str | float | None]]:
    """Extract simple finance-focused entities from an article text block."""

    lowered = text.lower()
    entities: list[dict[str, str | float | None]] = []
    seen: set[tuple[str, str]] = set()

    def add_entity(entity_text: str, entity_type: str, normalized: str | None = None) -> None:
        """Add one deduplicated entity record to the result list."""

        key = (entity_type, normalized or entity_text.lower())
        if key in seen:
            return
        seen.add(key)
        entities.append(
            {
                "entity_text": entity_text,
                "entity_type": entity_type,
                "normalized_value": normalized or entity_text.lower(),
                "confidence": 0.7,
            }
        )

    for match in ENTITY_PATTERNS["ticker"].finditer(text):
        token = match.group(0)
        if token.upper() in {"READ", "LINK"}:
            continue
        add_entity(token, "ticker", token.replace("$", "").upper())
    for collection, entity_type in [
        (COMPANIES, "company"),
        (EXCHANGES, "exchange"),
        (REGULATORS, "regulator"),
        (COUNTRIES, "region"),
        (CHAINS, "protocol"),
        (MACRO_TERMS, "macro_term"),
    ]:
        for item in collection:
            if item in lowered:
                add_entity(item, entity_type)
    return entities
