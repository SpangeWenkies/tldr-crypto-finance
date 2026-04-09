from __future__ import annotations

from pathlib import Path

import pytest

import tldr_crypto_finance.labeling.entities as entities_module
from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.db.duckdb import connect, init_database
from tldr_crypto_finance.labeling.entities import extract_entities
from tldr_crypto_finance.pipelines.enrich import label_articles


def _sql_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "src" / "tldr_crypto_finance" / "db"


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        database_path=tmp_path / "test.duckdb",
        raw_data_dir=tmp_path / "raw",
        curated_data_dir=tmp_path / "curated",
        config_dir=Path(__file__).resolve().parents[1] / "configs",
        entity_extraction_backend="heuristic",
    )


def test_extract_entities_finds_people_and_software_products() -> None:
    text = (
        "Coinbase CEO Brian Armstrong said ChatGPT and MetaMask are showing up in "
        "developer workflows alongside Stripe Radar and Ethereum."
    )

    entities = extract_entities(text)
    pairs = {(entity["entity_text"], entity["entity_type"]) for entity in entities}

    assert ("Brian Armstrong", "person") in pairs
    assert ("ChatGPT", "software_product") in pairs
    assert ("MetaMask", "software_product") in pairs
    assert ("Stripe Radar", "software_product") in pairs
    assert ("Coinbase", "company") in pairs
    assert ("Ethereum", "coin") in pairs
    assert ("Ethereum", "network") in pairs
    assert ("GPT", "ticker") not in pairs


def test_extract_entities_uses_context_for_coins_networks_and_countries() -> None:
    text = (
        "Ethereum developers deployed a bridge on the Ethereum network in Singapore. "
        "Later, Ethereum price rallied while Bitcoin ETF inflows rose in Brazil."
    )

    entities = extract_entities(text)
    pairs = {(entity["entity_text"], entity["entity_type"]) for entity in entities}

    assert ("Ethereum", "network") in pairs
    assert ("Ethereum", "coin") in pairs
    assert ("Bitcoin", "coin") in pairs
    assert ("Singapore", "country") in pairs
    assert ("Brazil", "country") in pairs


def test_extract_entities_expands_known_tickers_and_amms() -> None:
    text = (
        "AAPL and COIN rose while BTC, ETH, SOL, and UNI outperformed. "
        "Liquidity moved through Uniswap, Aerodrome, and Curve DEX pools across "
        "Base, Arbitrum, and Polygon."
    )

    entities = extract_entities(text)
    pairs = {(entity["entity_text"], entity["entity_type"]) for entity in entities}

    assert ("AAPL", "ticker") in pairs
    assert ("Apple", "company") in pairs
    assert ("COIN", "ticker") in pairs
    assert ("Coinbase", "company") in pairs
    assert ("BTC", "ticker") in pairs
    assert ("Bitcoin", "coin") in pairs
    assert ("ETH", "ticker") in pairs
    assert ("Ethereum", "coin") in pairs
    assert ("SOL", "ticker") in pairs
    assert ("Solana", "coin") in pairs
    assert ("UNI", "ticker") in pairs
    assert ("Uniswap", "amm") in pairs
    assert ("Aerodrome", "amm") in pairs
    assert ("Curve", "amm") in pairs
    assert ("Base", "network") in pairs
    assert ("Arbitrum", "network") in pairs
    assert ("Polygon", "network") in pairs


def test_extract_entities_with_ner_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_ner_pipeline(text: str):
        return [
            {"word": "Brian Armstrong", "entity_group": "PER", "score": 0.99},
            {"word": "Coinbase", "entity_group": "ORG", "score": 0.95},
            {"word": "Singapore", "entity_group": "LOC", "score": 0.92},
        ]

    monkeypatch.setattr(entities_module, "_load_ner_pipeline", lambda model_name: fake_ner_pipeline)

    entities = extract_entities(
        "Brian Armstrong said Coinbase is expanding in Singapore.",
        backend="ner",
        ner_model_name="stub-model",
    )
    pairs = {(entity["entity_text"], entity["entity_type"]) for entity in entities}

    assert ("Brian Armstrong", "person") in pairs
    assert ("Coinbase", "company") in pairs
    assert ("Coinbase", "exchange") in pairs
    assert ("Singapore", "country") in pairs


def test_extract_entities_with_hybrid_ner_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_ner_pipeline(text: str):
        return [
            {"word": "Patrick Collison", "entity_group": "PER", "score": 0.98},
            {"word": "Stripe", "entity_group": "ORG", "score": 0.94},
        ]

    monkeypatch.setattr(entities_module, "_load_ner_pipeline", lambda model_name: fake_ner_pipeline)

    entities = extract_entities(
        "Patrick Collison said Stripe uses GitHub Copilot with SOL on Solana.",
        backend="hybrid-ner",
        ner_model_name="stub-model",
    )
    pairs = {(entity["entity_text"], entity["entity_type"]) for entity in entities}

    assert ("Patrick Collison", "person") in pairs
    assert ("Stripe", "company") in pairs
    assert ("GitHub Copilot", "software_product") in pairs
    assert ("SOL", "ticker") in pairs
    assert ("Solana", "coin") in pairs


def test_label_articles_stores_people_and_software_entities(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    with connect(settings) as connection:
        init_database(connection, _sql_dir())
        connection.execute(
            """
            INSERT INTO article_blocks (
                article_id,
                issue_id,
                section_id,
                block_order,
                raw_block_text,
                clean_summary_text,
                extracted_title,
                title_confidence,
                canonical_url,
                primary_domain,
                is_sponsored_rule,
                is_sponsored_ml,
                sponsor_confidence,
                keep_for_analysis,
                parse_confidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "article-1",
                "issue-1",
                None,
                1,
                (
                    "Stripe CEO Patrick Collison said GitHub Copilot and Coinbase Wallet "
                    "are used by developers watching Bitcoin markets."
                ),
                (
                    "Stripe CEO Patrick Collison said GitHub Copilot and Coinbase Wallet "
                    "are used by developers watching Bitcoin markets."
                ),
                "Developer tools in crypto",
                0.9,
                None,
                "example.com",
                False,
                None,
                0.0,
                True,
                0.8,
            ],
        )

        stats = label_articles(connection, settings, sql_dir=_sql_dir(), mode="rules")
        rows = connection.execute(
            """
            SELECT entity_text, entity_type
            FROM article_entities
            WHERE article_id = 'article-1'
            ORDER BY entity_type, entity_text
            """
        ).fetchall()

    assert stats["entities_inserted"] >= 4
    assert ("Patrick Collison", "person") in rows
    assert ("GitHub Copilot", "software_product") in rows
    assert ("Coinbase Wallet", "software_product") in rows
    assert ("Bitcoin", "coin") in rows
