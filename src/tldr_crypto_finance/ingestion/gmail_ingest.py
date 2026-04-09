"""Optional Gmail API ingestion with checkpoint-based polling."""

from __future__ import annotations

import base64
from email import policy
from email.parser import BytesParser
from pathlib import Path

import duckdb

from tldr_crypto_finance.config import Settings
from tldr_crypto_finance.db.duckdb import begin_run, finish_run, init_database
from tldr_crypto_finance.ingestion.checkpoints import read_checkpoint, write_checkpoint
from tldr_crypto_finance.ingestion.common import insert_raw_message, parse_message_record


def _build_gmail_service(settings: Settings):
    """Create an authenticated Gmail API service object on demand."""

    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Gmail sync requires the optional gmail dependencies: pip install -e '.[gmail]'"
        ) from exc

    scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
    token_path = settings.gmail_token_path
    credentials_path = settings.gmail_credentials_path
    credentials = None
    if token_path.exists():
        credentials = Credentials.from_authorized_user_file(str(token_path), scopes=scopes)
    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    elif credentials is None or not credentials.valid:
        if not credentials_path.exists():
            msg = f"Gmail credentials file not found: {credentials_path}"
            raise RuntimeError(msg)
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), scopes=scopes)
        credentials = flow.run_local_server(port=0)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(credentials.to_json(), encoding="utf-8")
    return build("gmail", "v1", credentials=credentials, cache_discovery=False)


def _build_query(base_query: str, sender_filters: list[str], checkpoint_value: str | None) -> str:
    """Build the Gmail search query used for incremental polling."""

    parts = [base_query.strip()] if base_query.strip() else []
    if checkpoint_value:
        timestamp_seconds = int(int(checkpoint_value) / 1000)
        parts.append(f"after:{timestamp_seconds}")
    if sender_filters:
        sender_clause = " OR ".join(f"from:{sender}" for sender in sender_filters)
        parts.append(f"({sender_clause})")
    return " ".join(parts)


def sync_gmail(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    sql_dir: Path,
    sender_filters: list[str] | None = None,
    max_results: int = 100,
) -> dict[str, int]:
    """Poll Gmail for new messages and ingest them as raw records."""

    init_database(connection, sql_dir)
    run_id = begin_run(connection, "sync_gmail", notes=settings.gmail_query_filter)
    checkpoint_value = read_checkpoint(connection, "gmail", "last_internal_date_ms")
    service = _build_gmail_service(settings)
    stats = {"messages_seen": 0, "inserted": 0, "duplicates": 0, "errors": 0}
    highest_internal_date_ms = int(checkpoint_value or 0)

    try:
        query = _build_query(
            settings.gmail_query_filter,
            sender_filters or settings.default_sender_filters,
            checkpoint_value,
        )
        page_token = None
        while True:
            response = (
                service.users()
                .messages()
                .list(userId="me", q=query, maxResults=max_results, pageToken=page_token)
                .execute()
            )
            for item in response.get("messages", []):
                stats["messages_seen"] += 1
                message_id = item["id"]
                try:
                    payload = (
                        service.users()
                        .messages()
                        .get(userId="me", id=message_id, format="raw")
                        .execute()
                    )
                    raw_bytes = base64.urlsafe_b64decode(payload["raw"].encode("utf-8"))
                    message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
                    record = parse_message_record(
                        message,
                        source_system="gmail",
                        source_mailbox="gmail",
                        raw_path=None,
                        run_id=run_id,
                        source_message_id_override=message_id,
                    )
                    inserted, _ = insert_raw_message(connection, record)
                    stats["inserted" if inserted else "duplicates"] += 1
                    highest_internal_date_ms = max(
                        highest_internal_date_ms,
                        int(payload.get("internalDate", 0)),
                    )
                except Exception:
                    stats["errors"] += 1
            page_token = response.get("nextPageToken")
            if not page_token:
                break

        if highest_internal_date_ms:
            write_checkpoint(
                connection,
                "gmail",
                "last_internal_date_ms",
                str(highest_internal_date_ms),
            )
        finish_run(connection, run_id, "success", notes=str(stats))
    except Exception:
        finish_run(connection, run_id, "failed", notes=str(stats))
        raise
    return stats
