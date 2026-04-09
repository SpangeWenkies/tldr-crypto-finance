"""Time helpers for ingestion and reporting."""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime


def utc_now() -> datetime:
    """Return the current UTC timestamp as an aware datetime."""

    return datetime.now(tz=UTC)


def parse_email_datetime(value: str | None) -> datetime | None:
    """Parse RFC2822 email timestamps and normalize them to UTC when present."""

    if not value:
        return None
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
