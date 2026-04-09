"""Deterministic article labeling rules."""

from __future__ import annotations

import re
from collections.abc import Iterable

from tldr_crypto_finance.labeling.taxonomy import Taxonomy
from tldr_crypto_finance.models.records import LabelRecord

TOPIC_RULES = {
    "macro": ["inflation", "gdp", "labor market", "central bank", "recession"],
    "equities": ["earnings", "stocks", "share buyback", "valuations"],
    "credit": ["credit spreads", "default", "bond market", "high yield"],
    "commodities": ["oil", "gas", "gold", "copper", "commodity"],
    "crypto_markets": [
        "stablecoin",
        "exchange",
        "onchain",
        "token",
        "bitcoin",
        "ethereum",
        "custody",
        "crypto",
    ],
    "regulation": ["regulator", "enforcement", "rulemaking", "compliance", "regulation"],
    "fintech": ["payments", "fintech", "banking software", "embedded finance"],
    "venture": ["venture", "funding round", "series a", "startup"],
    "ai": ["artificial intelligence", "model release", "ai chip", "llm"],
    "cybersecurity": ["breach", "ransomware", "vulnerability", "incident response", "outage"],
    "geopolitics": ["sanctions", "trade war", "conflict", "industrial policy"],
    "company_news": ["guidance", "restructuring", "m&a", "chief executive"],
    "fraud_compliance": ["fraud", "money laundering", "aml", "controls"],
    "market_structure": ["exchange outage", "clearing", "settlement", "market structure"],
    "liquidity": ["liquidity", "funding stress", "deposit outflow", "margin call", "bank run"],
    "rates_fx": ["yield curve", "ecb", "fed", "dollar", "rates", "fx"],
}

SUBTOPIC_RULES = {
    "stablecoins": ["stablecoin", "reserve backing", "usdc", "tether"],
    "custody": ["custody", "custodian", "safeguarding"],
    "exchanges": ["exchange", "order book", "matching engine"],
    "inflation": ["inflation", "cpi", "pce", "price pressures"],
    "central_banks": ["fed", "ecb", "boj", "central bank"],
    "funding_stress": ["funding stress", "deposit outflow", "liquidity squeeze"],
    "breaches": ["breach", "data leak", "intrusion"],
    "resilience": ["incident response", "outage", "resilience"],
    "enforcement": ["enforcement", "fine", "probe", "settlement"],
}

ASSET_CLASS_RULES = {
    "crypto": ["stablecoin", "bitcoin", "ethereum", "exchange", "custody", "token"],
    "rates": ["fed", "ecb", "rates", "yield curve"],
    "fx": ["fx", "dollar", "yen", "carry"],
    "equities": ["stocks", "earnings", "shares"],
    "credit": ["bond", "default", "credit spreads"],
    "macro": ["inflation", "recession", "growth", "labor market"],
}

RISK_TYPE_RULES = {
    "liquidity": ["liquidity", "funding stress", "deposit outflow", "market depth"],
    "solvency": ["solvency", "insolvency", "restructuring", "default"],
    "regulatory": ["regulation", "regulator", "enforcement", "compliance"],
    "operational": ["outage", "incident response", "failure"],
    "cyber": ["breach", "exploit", "ransomware", "phishing"],
    "fraud": ["fraud", "scam", "money laundering", "aml"],
    "market": ["volatility", "selloff", "spread widening"],
    "counterparty": ["counterparty", "custodian", "prime broker"],
    "geopolitical": ["sanctions", "conflict", "trade war"],
}

REGION_RULES = {
    "united_states": ["fed", "sec", "cftc", "us ", "united states"],
    "europe": ["ecb", "europe", "eu", "esma", "uk"],
    "asia": ["boj", "china", "japan", "asia"],
    "emerging_markets": ["emerging markets", "argentina", "turkey", "nigeria"],
}

TONE_RULES = {
    "urgent": ["urgent", "stress", "warning", "crisis"],
    "cautious": ["watching", "pressure", "concern", "risk"],
    "bearish": ["selloff", "decline", "weak", "worse"],
    "bullish": ["rally", "strong", "upside"],
    "neutral": [],
}


def _count_phrase_hits(text: str, phrases: Iterable[str]) -> int:
    """Count distinct keyword hits using word-boundary-aware matching."""

    count = 0
    for phrase in phrases:
        pattern = rf"(?<!\w){re.escape(phrase.lower())}(?!\w)"
        if re.search(pattern, text):
            count += 1
    return count


def _best_label(text: str, mapping: dict[str, list[str]]) -> tuple[str | None, int]:
    """Return the highest scoring label and its hit count."""

    scores = {label: _count_phrase_hits(text, phrases) for label, phrases in mapping.items()}
    best_label = max(scores, key=scores.get)
    best_score = scores[best_label]
    return (best_label, best_score) if best_score > 0 else (None, 0)


def label_with_rules(article_id: str, text: str, taxonomy: Taxonomy) -> LabelRecord:
    """Assign labels from deterministic keyword rules."""

    lowered = text.lower()
    topic, topic_score = _best_label(lowered, TOPIC_RULES)
    if not taxonomy.has_topic(topic):
        topic = None
    subtopic, subtopic_score = _best_label(lowered, SUBTOPIC_RULES)
    if not taxonomy.has_subtopic(topic, subtopic):
        subtopic = None

    asset_class, _ = _best_label(lowered, ASSET_CLASS_RULES)
    if asset_class not in taxonomy.asset_classes:
        asset_class = None
    risk_type, _ = _best_label(lowered, RISK_TYPE_RULES)
    if risk_type not in taxonomy.risk_types:
        risk_type = None
    region, _ = _best_label(lowered, REGION_RULES)
    if region not in taxonomy.regions:
        region = "global"
    sentiment_tone, tone_score = _best_label(lowered, TONE_RULES)
    if sentiment_tone not in taxonomy.tones:
        sentiment_tone = "neutral"

    urgency = "high" if tone_score >= 1 and sentiment_tone == "urgent" else "normal"
    confidence = min(0.9, 0.35 + (topic_score * 0.15) + (subtopic_score * 0.05))
    return LabelRecord(
        article_id=article_id,
        topic=topic,
        subtopic=subtopic,
        asset_class=asset_class,
        risk_type=risk_type,
        region=region,
        sentiment_tone=sentiment_tone,
        urgency=urgency,
        label_source="rules",
        confidence=confidence if topic else 0.3,
    )
