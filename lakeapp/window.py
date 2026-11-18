from __future__ import annotations

from datetime import datetime, timedelta


def parse_ts(value: str) -> datetime:
    text = value.strip().replace(" ", "T")
    return datetime.fromisoformat(text)


def format_ts(value: datetime) -> str:
    return value.isoformat(timespec="seconds")


def window_start(occurred_at: str, minutes: int = 15) -> str:
    moment = parse_ts(occurred_at)
    bucket = moment.minute - moment.minute % minutes
    return format_ts(moment.replace(minute=bucket, second=0, microsecond=0))


def correction_deadline(window_start_text: str, correction_minutes: int) -> str:
    return format_ts(parse_ts(window_start_text) + timedelta(minutes=correction_minutes))


def needs_revision(published: bool, changed: bool) -> bool:
    return published and changed
