"""Optional IMAP ingestion for generic mailboxes and Proton Mail Bridge."""

from __future__ import annotations

import imaplib
from email import policy
from email.parser import BytesParser
from pathlib import Path

import duckdb

from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.db.duckdb import begin_run, finish_run, init_database
from tldr_crypto_finance.ingestion.checkpoints import read_checkpoint, write_checkpoint
from tldr_crypto_finance.ingestion.common import (
    decode_mime_header,
    insert_raw_message,
    parse_message_record,
)


def _matches_sender_filters(sender_header: str | None, sender_filters: list[str]) -> bool:
    """Return True when a sender passes the configured local sender filter list."""

    if not sender_filters:
        return True
    lowered = (decode_mime_header(sender_header) or "").lower()
    return any(fragment.lower() in lowered for fragment in sender_filters)


def sync_imap(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    sql_dir: Path,
    folder: str | None = None,
    sender_filters: list[str] | None = None,
    max_messages: int = 100,
) -> dict[str, int]:
    """Poll an IMAP mailbox incrementally using UID checkpoints."""

    init_database(connection, sql_dir)
    if not settings.imap_host or not settings.imap_username or not settings.imap_password:
        raise RuntimeError(
            "IMAP host, username, and password must be set before running sync-imap."
        )

    selected_folder = folder or settings.imap_folder
    run_id = begin_run(connection, "sync_imap", notes=selected_folder)
    checkpoint_key = f"{settings.imap_host}:{selected_folder}:last_uid"
    checkpoint_value = read_checkpoint(connection, "imap", checkpoint_key)
    stats = {"messages_seen": 0, "inserted": 0, "duplicates": 0, "errors": 0}
    highest_uid = int(checkpoint_value or 0)

    try:
        client = imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port)
        client.login(settings.imap_username, settings.imap_password)
        client.select(selected_folder, readonly=True)
        search_range = f"{highest_uid + 1}:*" if highest_uid else "1:*"
        status, data = client.uid("SEARCH", None, search_range)
        if status != "OK":
            raise RuntimeError(f"IMAP search failed for folder {selected_folder}")
        message_uids = data[0].decode("utf-8").split()
        for uid in message_uids[:max_messages]:
            stats["messages_seen"] += 1
            try:
                fetch_status, fetch_data = client.uid("FETCH", uid, "(RFC822)")
                if fetch_status != "OK" or not fetch_data or fetch_data[0] is None:
                    raise RuntimeError(f"IMAP fetch failed for UID {uid}")
                raw_bytes = fetch_data[0][1]
                message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
                if not _matches_sender_filters(
                    message.get("From"),
                    sender_filters or settings.default_sender_filters,
                ):
                    highest_uid = max(highest_uid, int(uid))
                    continue
                record = parse_message_record(
                    message,
                    source_system="imap",
                    source_mailbox=selected_folder,
                    raw_path=None,
                    run_id=run_id,
                    source_message_id_override=uid,
                )
                inserted, _ = insert_raw_message(connection, record)
                stats["inserted" if inserted else "duplicates"] += 1
                highest_uid = max(highest_uid, int(uid))
            except Exception:
                stats["errors"] += 1
        client.logout()
        if highest_uid:
            write_checkpoint(connection, "imap", checkpoint_key, str(highest_uid))
        finish_run(connection, run_id, "success", notes=str(stats))
    except Exception:
        finish_run(connection, run_id, "failed", notes=str(stats))
        raise
    return stats
