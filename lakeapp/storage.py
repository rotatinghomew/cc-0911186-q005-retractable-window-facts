from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_batches (
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (source, batch_id)
);
CREATE TABLE IF NOT EXISTS source_progress (
    source TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS facts (
    source TEXT NOT NULL,
    business_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    source_batch TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (source, business_key)
);
CREATE TABLE IF NOT EXISTS manifests (
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (name, version)
);
CREATE TABLE IF NOT EXISTS window_readings (
    source TEXT NOT NULL,
    business_key TEXT NOT NULL,
    device TEXT NOT NULL,
    window_start TEXT NOT NULL,
    value REAL NOT NULL,
    occurred_at TEXT NOT NULL,
    arrived_at TEXT NOT NULL,
    source_batch TEXT NOT NULL,
    state TEXT NOT NULL,
    PRIMARY KEY (source, business_key)
);
CREATE TABLE IF NOT EXISTS window_aggregates (
    source TEXT NOT NULL,
    device TEXT NOT NULL,
    window_start TEXT NOT NULL,
    event_min TEXT NOT NULL,
    event_max TEXT NOT NULL,
    arrival_min TEXT NOT NULL,
    arrival_max TEXT NOT NULL,
    reading_count INTEGER NOT NULL,
    value_sum REAL NOT NULL,
    value_min REAL NOT NULL,
    value_max REAL NOT NULL,
    version INTEGER NOT NULL,
    published_version INTEGER,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (source, device, window_start)
);
CREATE TABLE IF NOT EXISTS window_revisions (
    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    device TEXT NOT NULL,
    window_start TEXT NOT NULL,
    from_version INTEGER,
    to_version INTEGER NOT NULL,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    event_keys TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS unmerged_events (
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    business_key TEXT NOT NULL,
    device TEXT NOT NULL,
    kind TEXT NOT NULL,
    value REAL NOT NULL,
    occurred_at TEXT NOT NULL,
    arrived_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    resolve_note TEXT,
    PRIMARY KEY (source, business_key, kind, batch_id)
);
CREATE TABLE IF NOT EXISTS downstream_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    device TEXT NOT NULL,
    window_start TEXT NOT NULL,
    manifest_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    note TEXT
);
"""


def open_store(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    return connection


@contextmanager
def transaction(connection: sqlite3.Connection):
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def encode(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode(value: str):
    return json.loads(value)
