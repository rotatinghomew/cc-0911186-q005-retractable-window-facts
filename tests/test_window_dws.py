import tempfile
import unittest
from pathlib import Path

from lakeapp.dws import (
    list_unmerged,
    merge_events,
    publish_window,
    register_snapshot,
    resolve_snapshot,
    window_review,
)
from lakeapp.ingest import receive
from lakeapp.publish import read_manifest
from lakeapp.storage import open_store


class WindowBase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.connection = open_store(Path(self.tempdir.name) / "data.db")

    def tearDown(self):
        self.connection.close()
        self.tempdir.cleanup()

    def merge(self, source, batch_id, cursor, records, received_at, correction_minutes=15):
        events = receive(self.connection, source, batch_id, cursor, records, received_at)
        return merge_events(self.connection, events, correction_minutes)


class LateArrivalTest(WindowBase):
    def test_late_reading_within_correction_merges_into_event_window(self):
        result = self.merge(
            "telemetry",
            "batch-1",
            1,
            [
                {
                    "business_key": "r-1",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 10,
                    "occurred_at": "2026-11-01T09:02:00",
                }
            ],
            "2026-11-01T09:03:00",
        )
        self.assertEqual(result["merged"], 1)

        # 网关重连后补送 09:00 窗口的旧读数：到达时间更晚，但仍归属事件时间窗口
        result = self.merge(
            "telemetry",
            "batch-2",
            2,
            [
                {
                    "business_key": "r-2",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 20,
                    "occurred_at": "2026-11-01T09:14:00",
                }
            ],
            "2026-11-01T09:15:00",
        )
        self.assertEqual(result["merged"], 1)

        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        aggregate = review["aggregate"]
        self.assertEqual(aggregate["reading_count"], 2)
        self.assertEqual(aggregate["value_sum"], 30)
        # 双时间依据：事件时间范围与到达时间范围分别保留
        self.assertEqual(aggregate["event_min"], "2026-11-01T09:02:00")
        self.assertEqual(aggregate["event_max"], "2026-11-01T09:14:00")
        self.assertEqual(aggregate["arrival_min"], "2026-11-01T09:03:00")
        self.assertEqual(aggregate["arrival_max"], "2026-11-01T09:15:00")
        self.assertEqual(aggregate["status"], "草稿")

    def test_reading_beyond_correction_is_quarantined_not_silently_merged(self):
        self.merge(
            "telemetry",
            "batch-1",
            1,
            [
                {
                    "business_key": "r-1",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 10,
                    "occurred_at": "2026-11-01T09:02:00",
                }
            ],
            "2026-11-01T09:03:00",
        )
        result = self.merge(
            "telemetry",
            "batch-late",
            2,
            [
                {
                    "business_key": "r-late",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 99,
                    "occurred_at": "2026-11-01T09:10:00",
                }
            ],
            "2026-11-01T09:31:00",
        )
        self.assertEqual(result["quarantined"], 1)
        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(review["aggregate"]["reading_count"], 1)
        self.assertEqual(len(review["unmerged_events"]), 1)
        self.assertIn("超出更正范围", review["unmerged_events"][0]["reason"])
        self.assertEqual(review["unmerged_events"][0]["state"], "待归并")

    def test_merge_is_idempotent_for_replayed_batch(self):
        records = [
            {
                "business_key": "r-1",
                "device": "dev-1",
                "kind": "upsert",
                "value": 10,
                "occurred_at": "2026-11-01T09:02:00",
            }
        ]
        first = self.merge("telemetry", "batch-1", 1, records, "2026-11-01T09:03:00")
        second = self.merge("telemetry", "batch-1", 1, records, "2026-11-01T09:03:00")
        self.assertEqual(first["merged"], 1)
        self.assertEqual(second["merged"], 1)
        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(review["aggregate"]["version"], 1)
        self.assertEqual(review["revisions"], [])


class RetractRevisionTest(WindowBase):
    def _seed_published_window(self):
        self.merge(
            "telemetry",
            "batch-1",
            1,
            [
                {
                    "business_key": "r-1",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 10,
                    "occurred_at": "2026-11-01T09:02:00",
                },
                {
                    "business_key": "r-2",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 20,
                    "occurred_at": "2026-11-01T09:08:00",
                },
            ],
            "2026-11-01T09:09:00",
        )
        return publish_window(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")

    def test_retract_after_publish_creates_revision_and_flags_snapshot(self):
        published = self._seed_published_window()
        register_snapshot(
            self.connection,
            "alert-snap-1",
            "telemetry",
            "dev-1",
            "2026-11-01T09:00:00",
            note="告警快照",
        )
        self.merge(
            "telemetry",
            "batch-2",
            2,
            [
                {
                    "business_key": "r-2",
                    "device": "dev-1",
                    "kind": "retract",
                    "value": 0,
                    "occurred_at": "2026-11-01T09:08:00",
                }
            ],
            "2026-11-01T09:12:00",
        )
        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        aggregate = review["aggregate"]
        self.assertEqual(aggregate["version"], 2)
        self.assertEqual(aggregate["published_version"], 1)
        self.assertEqual(aggregate["status"], "待修订")
        self.assertEqual(aggregate["reading_count"], 1)
        self.assertEqual(aggregate["value_sum"], 10)

        # 撤回不是删除：明细保留并标记 retracted，事件不是“从未发生”
        reading = next(row for row in review["readings"] if row["business_key"] == "r-2")
        self.assertEqual(reading["state"], "retracted")

        # 修订缘由可追溯
        self.assertEqual(len(review["revisions"]), 1)
        revision = review["revisions"][0]
        self.assertEqual(revision["from_version"], 1)
        self.assertEqual(revision["to_version"], 2)
        self.assertEqual(revision["reason"], "撤回生效")
        self.assertIn("r-2", revision["event_keys"])

        # 已发布历史保持可读，没有被静默改写
        old = read_manifest(self.connection, published["name"], published["manifest_version"])
        self.assertEqual(old["rows"][0]["reading_count"], 2)
        self.assertEqual(old["rows"][0]["value_sum"], 30)

        # 下游告警快照显式标记需要修订
        self.assertEqual(review["snapshots"][0]["status"], "待修订")

    def test_republish_keeps_old_version_and_resolves_snapshot_flow(self):
        published = self._seed_published_window()
        register_snapshot(
            self.connection,
            "alert-snap-1",
            "telemetry",
            "dev-1",
            "2026-11-01T09:00:00",
        )
        self.merge(
            "telemetry",
            "batch-2",
            2,
            [
                {
                    "business_key": "r-2",
                    "device": "dev-1",
                    "kind": "retract",
                    "value": 0,
                    "occurred_at": "2026-11-01T09:08:00",
                }
            ],
            "2026-11-01T09:12:00",
        )
        republished = publish_window(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(republished["manifest_version"], published["manifest_version"] + 1)
        new_payload = read_manifest(self.connection, republished["name"], republished["manifest_version"])
        self.assertEqual(new_payload["rows"][0]["reading_count"], 1)

        # 旧版本仍可回读
        old_payload = read_manifest(self.connection, published["name"], published["manifest_version"])
        self.assertEqual(old_payload["rows"][0]["reading_count"], 2)

        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(review["aggregate"]["status"], "已发布")
        self.assertEqual(review["snapshots"][0]["status"], "已过时")
        resolve_snapshot(self.connection, "alert-snap-1", note="告警按新版本重算")
        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(review["snapshots"][0]["status"], "已修订")

    def test_retract_beyond_correction_does_not_touch_published_history(self):
        self._seed_published_window()
        result = self.merge(
            "telemetry",
            "batch-late",
            3,
            [
                {
                    "business_key": "r-1",
                    "device": "dev-1",
                    "kind": "retract",
                    "value": 0,
                    "occurred_at": "2026-11-01T09:02:00",
                }
            ],
            "2026-11-01T09:40:00",
        )
        self.assertEqual(result["quarantined"], 1)
        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(review["aggregate"]["version"], 1)
        self.assertEqual(review["aggregate"]["status"], "已发布")
        self.assertEqual(review["revisions"], [])
        self.assertEqual(list_unmerged(self.connection, "telemetry")[0]["reason"].startswith("超出更正范围"), True)

    def test_retract_with_unknown_attribution_is_quarantined(self):
        result = self.merge(
            "telemetry",
            "batch-x",
            9,
            [
                {
                    "business_key": "ghost",
                    "device": "dev-1",
                    "kind": "retract",
                    "value": 0,
                    "occurred_at": "2026-11-01T09:05:00",
                }
            ],
            "2026-11-01T09:06:00",
        )
        self.assertEqual(result["quarantined"], 1)
        unmerged = list_unmerged(self.connection, "telemetry")
        self.assertEqual(unmerged[0]["reason"], "撤回目标不存在，归属无法确认")


    def test_late_upsert_after_publish_creates_value_revision(self):
        self.merge(
            "telemetry",
            "batch-1",
            1,
            [
                {
                    "business_key": "r-1",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 10,
                    "occurred_at": "2026-11-01T09:02:00",
                }
            ],
            "2026-11-01T09:03:00",
        )
        publish_window(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        result = self.merge(
            "telemetry",
            "batch-2",
            2,
            [
                {
                    "business_key": "r-1",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 12,
                    "occurred_at": "2026-11-01T09:02:00",
                }
            ],
            "2026-11-01T09:14:00",
        )
        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["revised"], 1)
        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(review["aggregate"]["value_sum"], 12)
        self.assertEqual(review["aggregate"]["status"], "待修订")
        self.assertEqual(review["revisions"][0]["reason"], "迟到数据并入")
        self.assertEqual(review["revisions"][0]["event_keys"], ["r-1"])


class ReviewViewTest(WindowBase):
    def test_review_groups_current_summary_revisions_unmerged_and_snapshots(self):
        self.merge(
            "telemetry",
            "batch-1",
            1,
            [
                {
                    "business_key": "r-1",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 10,
                    "occurred_at": "2026-11-01T09:02:00",
                }
            ],
            "2026-11-01T09:03:00",
        )
        self.merge(
            "telemetry",
            "batch-late",
            2,
            [
                {
                    "business_key": "r-late",
                    "device": "dev-1",
                    "kind": "upsert",
                    "value": 99,
                    "occurred_at": "2026-11-01T09:10:00",
                }
            ],
            "2026-11-01T09:31:00",
        )
        publish_window(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        register_snapshot(
            self.connection,
            "alert-snap-1",
            "telemetry",
            "dev-1",
            "2026-11-01T09:00:00",
        )
        review = window_review(self.connection, "telemetry", "dev-1", "2026-11-01T09:00:00")
        self.assertEqual(set(review), {"source", "device", "window_start", "aggregate", "readings", "revisions", "unmerged_events", "snapshots"})
        self.assertEqual(review["aggregate"]["reading_count"], 1)
        self.assertEqual(len(review["unmerged_events"]), 1)
        self.assertEqual(review["snapshots"][0]["status"], "当前")
        self.assertEqual(review["revisions"], [])


if __name__ == "__main__":
    unittest.main()
