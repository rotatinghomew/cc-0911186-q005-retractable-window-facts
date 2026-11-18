"""DWS 窗口聚合：双时间依据、迟到更正、撤回留痕与下游快照状态。"""

from __future__ import annotations

from .contract import Event
from .ingest import arrival_of
from .publish import publish
from .storage import decode, encode, transaction
from .window import correction_deadline, parse_ts, window_start


def manifest_name(source: str, device: str, window_start_text: str) -> str:
    return f"window:{source}:{device}:{window_start_text}"


def merge_events(connection, events: list[Event], correction_minutes: int = 15) -> dict:
    """把事件并入事件时间窗口；超更正范围或归属不明的事件进入待归并区。

    更正范围按“事件所属窗口起点 + correction_minutes”与批次到达时间比较。
    已发布窗口在范围内被改写时产生修订版本，并把关联告警快照标记为待修订；
    超出范围的事件不会静默改写已发布历史。
    """
    summary = {"merged": 0, "updated": 0, "retracted": 0, "quarantined": 0, "revised": 0}
    touched: set[tuple[str, str, str]] = set()
    affected: dict[tuple[str, str, str], list[str]] = {}
    with transaction(connection):
        for event in events:
            arrived_at = arrival_of(connection, event.source, event.batch_id)
            outcome = _apply_event(connection, event, arrived_at, correction_minutes)
            summary[outcome["counted"]] += 1
            if outcome.get("window"):
                touched.add(outcome["window"])
                if outcome.get("business_key"):
                    affected.setdefault(outcome["window"], []).append(event.business_key)
        for key in sorted(touched):
            reason, keys = _recompute(connection, key, affected.get(key, []))
            if reason:
                summary["revised"] += 1
    summary["touched_windows"] = len(touched)
    return summary


def _apply_event(connection, event: Event, arrived_at: str, correction_minutes: int) -> dict:
    window_start_text = window_start(event.occurred_at)
    key = (event.source, event.device, window_start_text)
    deadline = correction_deadline(window_start_text, correction_minutes)
    if parse_ts(arrived_at) > parse_ts(deadline):
        _quarantine(connection, event, arrived_at, f"超出更正范围(截止{deadline})")
        return {"counted": "quarantined", "window": None}

    if event.kind == "retract":
        reading = connection.execute(
            "SELECT source,business_key FROM window_readings WHERE source=? AND business_key=?",
            (event.source, event.business_key),
        ).fetchone()
        if not reading:
            _quarantine(connection, event, arrived_at, "撤回目标不存在，归属无法确认")
            return {"counted": "quarantined", "window": None}
        existing = connection.execute(
            "SELECT device,window_start,state FROM window_readings WHERE source=? AND business_key=?",
            (event.source, event.business_key),
        ).fetchone()
        if existing["state"] == "retracted":
            return {"counted": "retracted", "window": (event.source, existing["device"], existing["window_start"])}
        connection.execute(
            "UPDATE window_readings SET state='retracted',source_batch=?,arrived_at=? "
            "WHERE source=? AND business_key=?",
            (event.batch_id, arrived_at, event.source, event.business_key),
        )
        return {
            "counted": "retracted",
            "window": (event.source, existing["device"], existing["window_start"]),
            "business_key": event.business_key,
            "reason": "撤回",
        }

    existing = connection.execute(
        "SELECT device,window_start,state,value FROM window_readings WHERE source=? AND business_key=?",
        (event.source, event.business_key),
    ).fetchone()
    if existing:
        expected_window = window_start_text
        if existing["window_start"] != expected_window:
            _quarantine(connection, event, arrived_at, "事件时间与既有窗口归属冲突")
            return {"counted": "quarantined", "window": None}
        if existing["state"] == "active" and float(existing["value"]) == event.value:
            return {"counted": "merged", "window": key}
        connection.execute(
            "UPDATE window_readings SET device=?,window_start=?,value=?,occurred_at=?,"
            "arrived_at=?,source_batch=?,state='active' WHERE source=? AND business_key=?",
            (
                event.device,
                window_start_text,
                event.value,
                event.occurred_at,
                arrived_at,
                event.batch_id,
                event.source,
                event.business_key,
            ),
        )
        reason = "撤回后重新到达" if existing["state"] == "retracted" else "迟到更正"
        return {"counted": "updated", "window": key, "business_key": event.business_key, "reason": reason}

    connection.execute(
        "INSERT INTO window_readings(source,business_key,device,window_start,value,occurred_at,"
        "arrived_at,source_batch,state) VALUES(?,?,?,?,?,?,?,?, 'active')",
        (
            event.source,
            event.business_key,
            event.device,
            window_start_text,
            event.value,
            event.occurred_at,
            arrived_at,
            event.batch_id,
        ),
    )
    return {"counted": "merged", "window": key, "business_key": event.business_key, "reason": "新增读数"}


def _quarantine(connection, event: Event, arrived_at: str, reason: str) -> None:
    connection.execute(
        "INSERT OR IGNORE INTO unmerged_events(source,batch_id,cursor,business_key,device,kind,"
        "value,occurred_at,arrived_at,reason,state,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?, '待归并', datetime('now'))",
        (
            event.source,
            event.batch_id,
            event.cursor,
            event.business_key,
            event.device,
            event.kind,
            event.value,
            event.occurred_at,
            arrived_at,
            reason,
        ),
    )


def _recompute(connection, key: tuple[str, str, str], affected_keys: list[str]) -> tuple[str | None, list[str]]:
    source, device, window_start_text = key
    rows = connection.execute(
        "SELECT business_key,value,occurred_at,arrived_at FROM window_readings "
        "WHERE source=? AND device=? AND window_start=? AND state='active' "
        "ORDER BY business_key",
        key,
    ).fetchall()
    values = [float(row["value"]) for row in rows]
    occurred = [row["occurred_at"] for row in rows]
    arrived = [row["arrived_at"] for row in rows]
    aggregate = connection.execute(
        "SELECT version,published_version,status,reading_count,value_sum FROM window_aggregates "
        "WHERE source=? AND device=? AND window_start=?",
        key,
    ).fetchone()

    if not rows:
        reading_count = 0
        value_sum = 0.0
        value_min = value_max = None
        event_min = event_max = None
        arrival_min = arrival_max = None
    else:
        reading_count = len(rows)
        value_sum = round(sum(values), 10)
        value_min = min(values)
        value_max = max(values)
        event_min, event_max = min(occurred), max(occurred)
        arrival_min, arrival_max = min(arrived), max(arrived)

    if aggregate is None:
        connection.execute(
            "INSERT INTO window_aggregates(source,device,window_start,event_min,event_max,"
            "arrival_min,arrival_max,reading_count,value_sum,value_min,value_max,version,"
            "published_version,status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,1,NULL,'草稿',datetime('now'))",
            (
                source,
                device,
                window_start_text,
                event_min,
                event_max,
                arrival_min,
                arrival_max,
                reading_count,
                value_sum,
                value_min,
                value_max,
            ),
        )
        return None, []

    unchanged = (
        aggregate["reading_count"] == reading_count
        and float(aggregate["value_sum"]) == value_sum
    )
    if unchanged:
        return None, []

    from_version = aggregate["version"]
    to_version = from_version + 1
    reason = "撤回生效" if reading_count < aggregate["reading_count"] else "迟到数据并入"
    event_keys = sorted({*affected_keys, *(row["business_key"] for row in rows)})
    connection.execute(
        "UPDATE window_aggregates SET event_min=?,event_max=?,arrival_min=?,arrival_max=?,"
        "reading_count=?,value_sum=?,value_min=?,value_max=?,version=?,status=?,"
        "updated_at=datetime('now') WHERE source=? AND device=? AND window_start=?",
        (
            event_min,
            event_max,
            arrival_min,
            arrival_max,
            reading_count,
            value_sum,
            value_min,
            value_max,
            to_version,
            "待修订" if aggregate["published_version"] is not None else "草稿",
            source,
            device,
            window_start_text,
        ),
    )
    connection.execute(
        "INSERT INTO window_revisions(source,device,window_start,from_version,to_version,reason,"
        "detail,event_keys,created_at) VALUES(?,?,?,?,?,?,?,?,datetime('now'))",
        (
            source,
            device,
            window_start_text,
            from_version,
            to_version,
            reason,
            encode(
                {
                    "reading_count": reading_count,
                    "value_sum": value_sum,
                    "event_min": event_min,
                    "event_max": event_max,
                    "arrival_min": arrival_min,
                    "arrival_max": arrival_max,
                }
            ),
            encode(event_keys),
        ),
    )
    if aggregate["published_version"] is not None:
        connection.execute(
            "UPDATE downstream_snapshots SET status='待修订',resolved_at=NULL "
            "WHERE source=? AND device=? AND window_start=? AND status='当前'",
            key,
        )
    return reason, event_keys


def publish_window(connection, source: str, device: str, window_start_text: str) -> dict:
    aggregate = connection.execute(
        "SELECT * FROM window_aggregates WHERE source=? AND device=? AND window_start=?",
        (source, device, window_start_text),
    ).fetchone()
    if not aggregate:
        raise KeyError("窗口汇总不存在")
    name = manifest_name(source, device, window_start_text)
    next_version = _next_manifest_version(connection, name)
    rows = [
        dict(row)
        for row in connection.execute(
            "SELECT business_key,value,occurred_at,arrived_at,state FROM window_readings "
            "WHERE source=? AND device=? AND window_start=? ORDER BY business_key",
            (source, device, window_start_text),
        )
    ]
    payload_rows = {
        "source": source,
        "device": device,
        "window_start": window_start_text,
        "aggregate_version": aggregate["version"],
        "event_min": aggregate["event_min"],
        "event_max": aggregate["event_max"],
        "arrival_min": aggregate["arrival_min"],
        "arrival_max": aggregate["arrival_max"],
        "reading_count": aggregate["reading_count"],
        "value_sum": aggregate["value_sum"],
        "value_min": aggregate["value_min"],
        "value_max": aggregate["value_max"],
        "readings": rows,
    }
    with transaction(connection):
        publish(connection, name, next_version, [payload_rows])
        connection.execute(
            "UPDATE window_aggregates SET published_version=?,status='已发布',updated_at=datetime('now') "
            "WHERE source=? AND device=? AND window_start=?",
            (aggregate["version"], source, device, window_start_text),
        )
        connection.execute(
            "UPDATE downstream_snapshots SET status='已过时' WHERE source=? AND device=? "
            "AND window_start=? AND status='待修订'",
            (source, device, window_start_text),
        )
    return {"name": name, "manifest_version": next_version, "aggregate_version": aggregate["version"]}


def _next_manifest_version(connection, name: str) -> int:
    row = connection.execute(
        "SELECT MAX(version) AS max_version FROM manifests WHERE name=?", (name,)
    ).fetchone()
    return (row["max_version"] or 0) + 1


def register_snapshot(
    connection,
    snapshot_id: str,
    source: str,
    device: str,
    window_start_text: str,
    note: str = "",
) -> dict:
    aggregate = connection.execute(
        "SELECT published_version FROM window_aggregates WHERE source=? AND device=? AND window_start=?",
        (source, device, window_start_text),
    ).fetchone()
    if not aggregate or aggregate["published_version"] is None:
        raise KeyError("窗口尚未发布，无法登记下游快照")
    with transaction(connection):
        connection.execute(
            "INSERT INTO downstream_snapshots(snapshot_id,source,device,window_start,manifest_version,"
            "status,created_at,note) VALUES(?,?,?,?,?, '当前', datetime('now'),?)",
            (snapshot_id, source, device, window_start_text, aggregate["published_version"], note),
        )
    return {"snapshot_id": snapshot_id, "status": "当前", "manifest_version": aggregate["published_version"]}


def resolve_snapshot(connection, snapshot_id: str, note: str = "") -> None:
    with transaction(connection):
        connection.execute(
            "UPDATE downstream_snapshots SET status='已修订',resolved_at=datetime('now'),note=? "
            "WHERE snapshot_id=?",
            (note, snapshot_id),
        )


def list_unmerged(connection, source: str | None = None) -> list[dict]:
    if source:
        rows = connection.execute(
            "SELECT * FROM unmerged_events WHERE source=? ORDER BY created_at,business_key",
            (source,),
        ).fetchall()
    else:
        rows = connection.execute(
            "SELECT * FROM unmerged_events ORDER BY created_at,business_key"
        ).fetchall()
    return [dict(row) for row in rows]


def resolve_unmerged(connection, source: str, business_key: str, kind: str, batch_id: str, note: str) -> dict:
    """运维人工复核待归并事件。

    超出更正范围或归属无法确认的事件不得回写已发布历史，只能记录复核结论；
    复核通过的隔离事件标记为“已归并”，表示已通过重新下发的合规批次处理，
    复核驳回或确认无需处理的标记为“已忽略”。
    """
    row = connection.execute(
        "SELECT * FROM unmerged_events WHERE source=? AND business_key=? AND kind=? AND batch_id=?",
        (source, business_key, kind, batch_id),
    ).fetchone()
    if not row:
        raise KeyError("待归并事件不存在")
    if row["state"] != "待归并":
        return {"state": row["state"], "note": "事件已处理"}
    with transaction(connection):
        connection.execute(
            "UPDATE unmerged_events SET state='已忽略',resolved_at=datetime('now'),resolve_note=? "
            "WHERE source=? AND business_key=? AND kind=? AND batch_id=?",
            (note, source, business_key, kind, batch_id),
        )
    return {"state": "已忽略", "note": note}


def window_review(connection, source: str, device: str, window_start_text: str) -> dict:
    aggregate = connection.execute(
        "SELECT * FROM window_aggregates WHERE source=? AND device=? AND window_start=?",
        (source, device, window_start_text),
    ).fetchone()
    revisions = [
        dict(row)
        for row in connection.execute(
            "SELECT revision_id,from_version,to_version,reason,detail,event_keys,created_at "
            "FROM window_revisions WHERE source=? AND device=? AND window_start=? ORDER BY revision_id",
            (source, device, window_start_text),
        )
    ]
    for revision in revisions:
        revision["detail"] = decode(revision["detail"])
        revision["event_keys"] = decode(revision["event_keys"])
    unmerged = [
        dict(row)
        for row in connection.execute(
            "SELECT business_key,kind,value,occurred_at,arrived_at,reason,state,created_at,"
            "resolved_at,resolve_note FROM unmerged_events WHERE source=? AND device=? ORDER BY created_at",
            (source, device),
        )
    ]
    snapshots = [
        dict(row)
        for row in connection.execute(
            "SELECT snapshot_id,manifest_version,status,created_at,resolved_at,note "
            "FROM downstream_snapshots WHERE source=? AND device=? AND window_start=? "
            "ORDER BY created_at",
            (source, device, window_start_text),
        )
    ]
    readings = [
        dict(row)
        for row in connection.execute(
            "SELECT business_key,value,occurred_at,arrived_at,state,source_batch "
            "FROM window_readings WHERE source=? AND device=? AND window_start=? ORDER BY business_key",
            (source, device, window_start_text),
        )
    ]
    return {
        "source": source,
        "device": device,
        "window_start": window_start_text,
        "aggregate": dict(aggregate) if aggregate else None,
        "readings": readings,
        "revisions": revisions,
        "unmerged_events": unmerged,
        "snapshots": snapshots,
    }
