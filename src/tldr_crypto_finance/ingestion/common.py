"""Shared ingestion helpers for raw email messages."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from email.header import decode_header
from email.message import Message
from email.utils import parseaddr

import duckdb
from bs4 import BeautifulSoup

from tldr_crypto_finance.models.records import RawMessageRecord
from tldr_crypto_finance.utils.hashing import stable_hash
from tldr_crypto_finance.utils.text import collapse_whitespace
from tldr_crypto_finance.utils.time import parse_email_datetime, utc_now

logger = logging.getLogger(__name__)


def decode_mime_header(value: str | None) -> str | None:
    """Decode RFC 2047 headers into readable Unicode text."""

    if not value:
        return None
    parts: list[str] = []
    for chunk, encoding in decode_header(value):
        if isinstance(chunk, bytes):
            parts.append(chunk.decode(encoding or "utf-8", errors="replace"))
        else:
            parts.append(chunk)
    return "".join(parts).strip() or None


def _decode_payload(part: Message) -> str:
    """Decode a mail part to text while tolerating malformed charsets and bodies."""

    if hasattr(part, "get_content"):
        try:
            content = part.get_content()
            if isinstance(content, str):
                return content
            if isinstance(content, bytes):
                return content.decode(part.get_content_charset() or "utf-8", errors="replace")
        except (LookupError, ValueError):
            logger.debug("Falling back to payload decode for malformed part", exc_info=True)
    payload = part.get_payload(decode=True)
    if payload is None:
        raw_payload = part.get_payload()
        return raw_payload if isinstance(raw_payload, str) else ""
    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")


def extract_bodies(message: Message) -> tuple[str, str]:
    """Extract text and HTML bodies from an email, skipping attachments."""

    text_parts: list[str] = []
    html_parts: list[str] = []
    walk_parts: Iterable[Message] = message.walk() if message.is_multipart() else [message]

    for part in walk_parts:
        if part.is_multipart():
            continue
        if (part.get_content_disposition() or "").lower() == "attachment":
            continue
        content_type = part.get_content_type()
        payload = _decode_payload(part).strip()
        if not payload:
            continue
        if content_type == "text/plain":
            text_parts.append(payload)
        elif content_type == "text/html":
            html_parts.append(payload)

    text_body = "\n\n".join(text_parts).strip()
    html_body = "\n\n".join(html_parts).strip()
    if not text_body and html_body:
        text_body = BeautifulSoup(html_body, "html.parser").get_text("\n", strip=True)
    return text_body, html_body


def build_internal_message_pk(
    source_system: str,
    source_message_id: str | None,
    body_hash: str,
    sender_email: str | None,
    subject: str | None,
    sent_at: str | None,
) -> str:
    """Build a stable primary key used inside the local database."""

    return stable_hash(
        source_system,
        source_message_id or "",
        body_hash,
        sender_email or "",
        subject or "",
        sent_at or "",
    )


def parse_message_record(
    message: Message,
    *,
    source_system: str,
    source_mailbox: str | None,
    raw_path: str | None,
    run_id: str,
    source_message_id_override: str | None = None,
) -> RawMessageRecord:
    """Normalize a parsed email message into the raw message table shape."""

    sender_name, sender_email = parseaddr(decode_mime_header(message.get("From")) or "")
    subject = decode_mime_header(message.get("Subject"))
    source_message_id = source_message_id_override or decode_mime_header(message.get("Message-ID"))
    text_body, html_body = extract_bodies(message)
    normalized_body = collapse_whitespace(text_body or html_body)
    body_hash = stable_hash(normalized_body)
    sent_at = parse_email_datetime(message.get("Date"))
    received_at = parse_email_datetime(message.get("Delivered-To-Date")) or sent_at
    internal_message_pk = build_internal_message_pk(
        source_system=source_system,
        source_message_id=source_message_id,
        body_hash=body_hash,
        sender_email=sender_email or None,
        subject=subject,
        sent_at=sent_at.isoformat() if sent_at else None,
    )
    return RawMessageRecord(
        internal_message_pk=internal_message_pk,
        source_system=source_system,
        source_message_id=source_message_id,
        source_mailbox=source_mailbox,
        sender_name=sender_name or None,
        sender_email=sender_email or None,
        subject=subject,
        sent_at=sent_at,
        received_at=received_at,
        ingested_at=utc_now(),
        text_body=text_body,
        html_body=html_body,
        body_hash=body_hash,
        raw_path=raw_path,
        run_id=run_id,
    )


def find_existing_message(
    connection: duckdb.DuckDBPyConnection,
    record: RawMessageRecord,
) -> str | None:
    """Find an existing raw message using Message-ID first and body hash as fallback."""

    if record.source_message_id:
        row = connection.execute(
            """
            SELECT internal_message_pk
            FROM raw_messages
            WHERE source_system = ? AND source_message_id = ?
            LIMIT 1
            """,
            [record.source_system, record.source_message_id],
        ).fetchone()
        if row is not None:
            return str(row[0])

    row = connection.execute(
        """
        SELECT internal_message_pk
        FROM raw_messages
        WHERE source_system = ?
          AND body_hash = ?
          AND coalesce(sender_email, '') = coalesce(?, '')
          AND coalesce(subject, '') = coalesce(?, '')
        LIMIT 1
        """,
        [record.source_system, record.body_hash, record.sender_email, record.subject],
    ).fetchone()
    return None if row is None else str(row[0])


def insert_raw_message(
    connection: duckdb.DuckDBPyConnection,
    record: RawMessageRecord,
) -> tuple[bool, str]:
    """Insert a raw message when it is not already present."""

    existing_pk = find_existing_message(connection, record)
    if existing_pk:
        return False, existing_pk

    connection.execute(
        """
        INSERT INTO raw_messages (
            internal_message_pk,
            source_system,
            source_message_id,
            source_mailbox,
            sender_name,
            sender_email,
            subject,
            sent_at,
            received_at,
            ingested_at,
            text_body,
            html_body,
            body_hash,
            raw_path,
            run_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            record.internal_message_pk,
            record.source_system,
            record.source_message_id,
            record.source_mailbox,
            record.sender_name,
            record.sender_email,
            record.subject,
            record.sent_at,
            record.received_at,
            record.ingested_at,
            record.text_body,
            record.html_body,
            record.body_hash,
            record.raw_path,
            record.run_id,
        ],
    )
    return True, record.internal_message_pk
