from __future__ import annotations


def window_start(occurred_at: str, minutes: int = 15) -> str:
    hour, minute = map(int, occurred_at[11:16].split(':'))
    bucket = minute - minute % minutes
    return f"{occurred_at[:11]}{hour:02d}:{bucket:02d}:00"


def needs_revision(published: bool, changed: bool) -> bool:
    return published and changed
