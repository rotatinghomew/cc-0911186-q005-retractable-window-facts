from __future__ import annotations


def revision_state(published: bool, changed: bool) -> str:
    if published and changed:
        return "待修订"
    return "当前"
