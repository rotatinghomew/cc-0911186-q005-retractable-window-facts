from __future__ import annotations

import argparse
import json

from .derive import apply_events, facts
from .dws import (
    list_unmerged,
    merge_events,
    publish_window,
    register_snapshot,
    resolve_snapshot,
    resolve_unmerged,
)
from .ingest import progress, receive
from .publish import publish, read_manifest
from .query import window_review
from .storage import open_store


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="本地分层数据服务")
    parser.add_argument("database")
    parser.add_argument(
        "action",
        choices=[
            "receive",
            "facts",
            "progress",
            "publish",
            "read",
            "window-merge",
            "window-publish",
            "snapshot-register",
            "snapshot-resolve",
            "unmerged",
            "review",
        ],
    )
    parser.add_argument("payload", nargs="?")
    args = parser.parse_args(argv)
    connection = open_store(args.database)
    try:
        if args.action == "receive":
            data = json.loads(args.payload or "{}")
            events = receive(
                connection,
                data["source"],
                data["batch_id"],
                data["cursor"],
                data["records"],
                data.get("received_at"),
            )
            print(json.dumps({"received": len(events), "changed": apply_events(connection, events)}, ensure_ascii=False))
        elif args.action == "facts":
            print(json.dumps(facts(connection), ensure_ascii=False))
        elif args.action == "progress":
            print(json.dumps(progress(connection), ensure_ascii=False))
        elif args.action == "publish":
            data = json.loads(args.payload or "{}")
            publish(connection, data["name"], data["version"], facts(connection))
            print("ok")
        elif args.action == "read":
            data = json.loads(args.payload or "{}")
            print(json.dumps(read_manifest(connection, data["name"], data["version"]), ensure_ascii=False))
        elif args.action == "window-merge":
            data = json.loads(args.payload or "{}")
            events = receive(
                connection,
                data["source"],
                data["batch_id"],
                data["cursor"],
                data["records"],
                data.get("received_at"),
            )
            result = merge_events(connection, events, int(data.get("correction_minutes", 15)))
            print(json.dumps(result, ensure_ascii=False))
        elif args.action == "window-publish":
            data = json.loads(args.payload or "{}")
            print(
                json.dumps(
                    publish_window(connection, data["source"], data["device"], data["window_start"]),
                    ensure_ascii=False,
                )
            )
        elif args.action == "snapshot-register":
            data = json.loads(args.payload or "{}")
            print(
                json.dumps(
                    register_snapshot(
                        connection,
                        data["snapshot_id"],
                        data["source"],
                        data["device"],
                        data["window_start"],
                        data.get("note", ""),
                    ),
                    ensure_ascii=False,
                )
            )
        elif args.action == "snapshot-resolve":
            data = json.loads(args.payload or "{}")
            resolve_snapshot(connection, data["snapshot_id"], data.get("note", ""))
            print("ok")
        elif args.action == "unmerged":
            data = json.loads(args.payload or "{}")
            print(json.dumps(list_unmerged(connection, data.get("source")), ensure_ascii=False))
        else:
            data = json.loads(args.payload or "{}")
            print(
                json.dumps(
                    window_review(connection, data["source"], data["device"], data["window_start"]),
                    ensure_ascii=False,
                )
            )
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
