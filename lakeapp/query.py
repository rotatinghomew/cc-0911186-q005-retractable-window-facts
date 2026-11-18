from __future__ import annotations

from .dws import window_review as _window_review


def revision_state(published: bool, changed: bool) -> str:
    if published and changed:
        return "待修订"
    return "当前"


def window_review(connection, source: str, device: str, window_start_text: str) -> dict:
    """按设备和窗口返回当前汇总、修订缘由、未归并事件与下游快照处理状态。"""
    return _window_review(connection, source, device, window_start_text)
