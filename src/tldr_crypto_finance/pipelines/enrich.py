"""Issue parsing and article extraction pipeline."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb

from tldr_crypto_finance.config import Settings, load_yaml_config
from tldr_crypto_finance.db.duckdb import begin_run, finish_run, init_database
from tldr_crypto_finance.labeling.entities import extract_entities
from tldr_crypto_finance.labeling.review_queue import clear_label_reviews, enqueue_label_review
from tldr_crypto_finance.labeling.rules import label_with_rules
from tldr_crypto_finance.labeling.taxonomy import load_taxonomy
from tldr_crypto_finance.labeling.zero_shot import zero_shot_topic_label
from tldr_crypto_finance.parsing.article_splitter import extract_title, split_article_blocks
from tldr_crypto_finance.parsing.html_clean import pick_preferred_body
from tldr_crypto_finance.parsing.link_extractor import extract_links
from tldr_crypto_finance.parsing.normalize import normalize_newsletter_text
from tldr_crypto_finance.parsing.section_splitter import split_sections
from tldr_crypto_finance.parsing.sender_profile_parser import SenderProfileCatalog
from tldr_crypto_finance.parsing.sponsor_filter import detect_sponsorship
from tldr_crypto_finance.utils.hashing import stable_hash
from tldr_crypto_finance.utils.text import strip_urls
from tldr_crypto_finance.utils.urls import extract_domain


def _sql_limit_clause(limit: int | None) -> str:
    return "" if limit is None else f" LIMIT {int(limit)}"


def _fetch_raw_messages(
    connection: duckdb.DuckDBPyConnection,
    *,
    force: bool,
    limit: int | None,
) -> list[tuple]:
    """Fetch raw messages that still need parsing, or all messages when forced."""

    where_clause = "1 = 1" if force else "ni.internal_message_pk IS NULL"
    query = f"""
        SELECT
            rm.internal_message_pk,
            rm.sender_name,
            rm.sender_email,
            rm.subject,
            rm.sent_at,
            rm.text_body,
            rm.html_body
        FROM raw_messages rm
        LEFT JOIN newsletter_issues ni ON ni.internal_message_pk = rm.internal_message_pk
        WHERE {where_clause}
        ORDER BY rm.sent_at ASC NULLS LAST, rm.ingested_at ASC
        {_sql_limit_clause(limit)}
    """
    return connection.execute(query).fetchall()


def _delete_existing_issue_parse(connection: duckdb.DuckDBPyConnection, issue_id: str) -> None:
    """Delete prior parsed artifacts for an issue before reparsing it."""

    article_ids = [
        row[0]
        for row in connection.execute(
            "SELECT article_id FROM article_blocks WHERE issue_id = ?",
            [issue_id],
        ).fetchall()
    ]
    if article_ids:
        placeholders = ", ".join(["?"] * len(article_ids))
        connection.execute(
            f"DELETE FROM article_links WHERE article_id IN ({placeholders})",
            article_ids,
        )
        connection.execute(
            f"DELETE FROM manual_review_queue WHERE article_id IN ({placeholders})",
            article_ids,
        )
        connection.execute(
            f"DELETE FROM article_entities WHERE article_id IN ({placeholders})",
            article_ids,
        )
        connection.execute(
            f"DELETE FROM article_labels WHERE article_id IN ({placeholders})",
            article_ids,
        )
    connection.execute("DELETE FROM article_blocks WHERE issue_id = ?", [issue_id])
    connection.execute("DELETE FROM sections WHERE issue_id = ?", [issue_id])
    connection.execute("DELETE FROM newsletter_issues WHERE issue_id = ?", [issue_id])


def parse_issues(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    sql_dir: Path,
    force: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Parse raw messages into issues, sections, article blocks, and links."""

    init_database(connection, sql_dir)
    sponsor_rules = load_yaml_config(settings.config_path("sponsor_rules.yml"))
    profile_catalog = SenderProfileCatalog.from_config(
        load_yaml_config(settings.config_path("sender_profiles.yml"))
    )
    run_id = begin_run(connection, "parse_issues", notes=f"force={force}, limit={limit}")
    stats = {
        "messages_parsed": 0,
        "sections_inserted": 0,
        "articles_inserted": 0,
        "sponsor_flagged": 0,
        "review_items_created": 0,
    }

    try:
        for row in _fetch_raw_messages(connection, force=force, limit=limit):
            (
                internal_message_pk,
                sender_name,
                sender_email,
                _subject,
                sent_at,
                text_body,
                html_body,
            ) = row
            issue_id = stable_hash("issue", internal_message_pk)
            _delete_existing_issue_parse(connection, issue_id)
            profile = profile_catalog.select(sender_email, sender_name)
            newsletter_name, newsletter_slug = profile_catalog.newsletter_identity(
                profile,
                sender_name,
                sender_email,
            )
            edition_date = sent_at.date() if isinstance(sent_at, datetime) else None
            source_text = pick_preferred_body(text_body or "", html_body or "")
            normalized_text = normalize_newsletter_text(source_text)
            sections = split_sections(normalized_text, profile.section_patterns)
            if not sections and normalized_text:
                sections = split_sections(f"General\n\n{normalized_text}", profile.section_patterns)

            connection.execute(
                """
                INSERT INTO newsletter_issues (
                    issue_id,
                    internal_message_pk,
                    newsletter_name,
                    newsletter_slug,
                    edition_date
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [issue_id, internal_message_pk, newsletter_name, newsletter_slug, edition_date],
            )

            block_counter = 0
            for section in sections:
                section_id = stable_hash(issue_id, section.order, section.name)
                connection.execute(
                    """
                    INSERT INTO sections (section_id, issue_id, section_name, section_order)
                    VALUES (?, ?, ?, ?)
                    """,
                    [section_id, issue_id, section.name, section.order],
                )
                stats["sections_inserted"] += 1

                for block_text in split_article_blocks(
                    section.raw_text,
                    min_words=profile.article_split_min_words,
                ):
                    block_counter += 1
                    links = extract_links(block_text)
                    canonical_url = (
                        str(links[0]["canonical_url"])
                        if links and links[0].get("canonical_url")
                        else None
                    )
                    title, title_confidence = extract_title(block_text)
                    clean_summary_text = strip_urls(block_text)
                    sponsor_decision = detect_sponsorship(block_text, links, sponsor_rules)
                    article_id = stable_hash(
                        issue_id,
                        section_id,
                        block_counter,
                        canonical_url or clean_summary_text,
                    )
                    parse_confidence = min(
                        0.95,
                        0.35
                        + (0.25 if links else 0.0)
                        + (0.2 if title else 0.0)
                        + (0.15 if len(clean_summary_text.split()) >= 20 else 0.0),
                    )
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
                            article_id,
                            issue_id,
                            section_id,
                            block_counter,
                            block_text,
                            clean_summary_text,
                            title,
                            title_confidence,
                            canonical_url,
                            extract_domain(canonical_url) if canonical_url else None,
                            sponsor_decision.is_sponsored,
                            None,
                            sponsor_decision.confidence,
                            not sponsor_decision.is_sponsored,
                            parse_confidence,
                        ],
                    )
                    stats["articles_inserted"] += 1
                    if sponsor_decision.is_sponsored:
                        stats["sponsor_flagged"] += 1

                    for link in links:
                        connection.execute(
                            """
                            INSERT INTO article_links (
                                article_id,
                                original_url,
                                canonical_url,
                                domain,
                                link_text,
                                link_order
                            )
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            [
                                article_id,
                                link["original_url"],
                                link["canonical_url"],
                                link["domain"],
                                link["link_text"],
                                link["link_order"],
                            ],
                        )
                    if sponsor_decision.ambiguous:
                        review_id = stable_hash(article_id, "sponsor", sponsor_decision.reason)
                        connection.execute(
                            """
                            INSERT INTO manual_review_queue (
                                review_id,
                                article_id,
                                review_reason,
                                current_status,
                                created_at
                            )
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            [
                                review_id,
                                article_id,
                                f"sponsor:{sponsor_decision.reason}",
                                "open",
                                datetime.now(tz=UTC),
                            ],
                        )
                        stats["review_items_created"] += 1
            stats["messages_parsed"] += 1
        finish_run(connection, run_id, "success", notes=str(stats))
    except Exception:
        finish_run(connection, run_id, "failed", notes=str(stats))
        raise
    return stats


def _fetch_articles_to_label(
    connection: duckdb.DuckDBPyConnection,
    *,
    force: bool,
    limit: int | None,
) -> list[tuple]:
    """Fetch article blocks that still need labels or should be relabeled."""

    where_clause = "ab.keep_for_analysis = TRUE"
    if not force:
        where_clause += " AND al.article_id IS NULL"
    query = f"""
        SELECT
            ab.article_id,
            ab.clean_summary_text,
            coalesce(ab.extracted_title, ''),
            coalesce(ab.primary_domain, '')
        FROM article_blocks ab
        LEFT JOIN article_labels al ON al.article_id = ab.article_id
        WHERE {where_clause}
        ORDER BY ab.article_id
        {_sql_limit_clause(limit)}
    """
    return connection.execute(query).fetchall()


def label_articles(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    sql_dir: Path,
    mode: str = "rules",
    force: bool = False,
    limit: int | None = None,
) -> dict[str, int]:
    """Label article blocks, extract entities, and update manual review items."""

    init_database(connection, sql_dir)
    taxonomy = load_taxonomy(settings)
    run_id = begin_run(connection, "label_articles", notes=f"mode={mode}, force={force}")
    stats = {"labeled": 0, "entities_inserted": 0, "review_items_created": 0}

    try:
        rows = _fetch_articles_to_label(connection, force=force, limit=limit)
        for article_id, clean_summary_text, extracted_title, primary_domain in rows:
            if force:
                connection.execute("DELETE FROM article_labels WHERE article_id = ?", [article_id])
                connection.execute(
                    "DELETE FROM article_entities WHERE article_id = ?",
                    [article_id],
                )
                clear_label_reviews(connection, article_id)

            text = " ".join(
                part for part in [extracted_title, clean_summary_text, primary_domain] if part
            )
            label = label_with_rules(article_id, text, taxonomy)
            if mode in {"zero-shot", "hybrid"}:
                try:
                    zs_topic, zs_subtopic, zs_confidence = zero_shot_topic_label(
                        text,
                        taxonomy,
                        settings.zero_shot_model_name,
                    )
                except RuntimeError:
                    if mode == "zero-shot":
                        raise
                else:
                    if mode == "zero-shot" or (label.topic is None or label.confidence < 0.55):
                        label.topic = zs_topic
                        label.subtopic = zs_subtopic
                        label.label_source = mode
                        label.confidence = max(label.confidence, zs_confidence)

            connection.execute(
                """
                INSERT INTO article_labels (
                    article_id,
                    topic,
                    subtopic,
                    asset_class,
                    risk_type,
                    region,
                    sentiment_tone,
                    urgency,
                    label_source,
                    confidence
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    label.article_id,
                    label.topic,
                    label.subtopic,
                    label.asset_class,
                    label.risk_type,
                    label.region,
                    label.sentiment_tone,
                    label.urgency,
                    label.label_source,
                    label.confidence,
                ],
            )
            for entity in extract_entities(text):
                connection.execute(
                    """
                    INSERT INTO article_entities (
                        article_id,
                        entity_text,
                        entity_type,
                        normalized_value,
                        confidence
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        article_id,
                        entity["entity_text"],
                        entity["entity_type"],
                        entity["normalized_value"],
                        entity["confidence"],
                    ],
                )
                stats["entities_inserted"] += 1

            if label.topic is None:
                enqueue_label_review(connection, article_id, "label:topic_missing")
                stats["review_items_created"] += 1
            elif label.confidence < 0.5:
                enqueue_label_review(connection, article_id, "label:low_confidence")
                stats["review_items_created"] += 1
            stats["labeled"] += 1
        finish_run(connection, run_id, "success", notes=str(stats))
    except Exception:
        finish_run(connection, run_id, "failed", notes=str(stats))
        raise
    return stats
