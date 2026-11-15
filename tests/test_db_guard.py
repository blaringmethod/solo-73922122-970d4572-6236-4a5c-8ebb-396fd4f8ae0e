"""发布守卫的单元测试：旧版本 / 旧运行不得覆盖已发布清单。"""
from __future__ import annotations

import pytest

from app import db


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db.config.DB_PATH = tmp_path / "test.db"
    db.config.MANIFEST_PATH = tmp_path / "manifest.json"
    db.init_db()
    yield


def _seed(run_id: int, version: int, batches: list[tuple[str, str]]) -> None:
    db.stage_announcements(run_id, version, [
        {"announcement_id": f"A{run_id}", "title": f"公告{run_id}",
         "published_at": None, "withdrawn": False, "withdraw_reason": None,
         "detail_url": "/x", "attachment_url": "/y.csv"},
    ])
    db.stage_batches(run_id, [
        {"announcement_id": f"A{run_id}", "product_code": code, "batch_no": batch,
         "product_name": f"产品{code}", "produced_at": None, "reason": "测试"}
        for code, batch in batches
    ])


def test_older_version_commit_rejected():
    run_v2 = db.create_run(2)
    _seed(run_v2, 2, [("P1", "B1"), ("P1", "B2")])
    db.commit_run(run_v2, 2)
    assert db.get_published_meta()["source_version"] == 2

    run_v1 = db.create_run(1)
    _seed(run_v1, 1, [("P9", "B9")])
    with pytest.raises(db.StalePublishError):
        db.commit_run(run_v1, 1)

    # 正式清单与 JSON 文件都停留在 v2
    assert db.get_published_meta()["source_version"] == 2
    assert {e["batch_no"] for e in db.list_manifest()} == {"B1", "B2"}
    assert db.get_run(run_v1)["status"] == "running"  # 由编排层标记 stale


def test_same_version_older_run_rejected_but_newer_allowed():
    first = db.create_run(1)
    _seed(first, 1, [("P1", "B1")])
    db.commit_run(first, 1)

    # 构造“同版本但 run_id 更小”的边界提交（自增 ID 下不会自然出现，守卫仍须拦截）
    older = first - 1
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO collection_runs (id, started_at, status, target_version) "
            "VALUES (?, ?, 'running', 1)",
            (older, db.utc_now_iso()),
        )
    _seed(older, 1, [("P1", "B1")])
    with pytest.raises(db.StalePublishError):
        db.commit_run(older, 1)

    # 同版本下更新的运行允许发布（运行时撤回场景）
    newer = db.create_run(1)
    _seed(newer, 1, [("P1", "B1"), ("P2", "B2")])
    payload = db.commit_run(newer, 1)
    assert db.get_published_meta()["published_run_id"] == newer
    assert payload["entry_count"] == 2


def test_failed_commit_keeps_previous_manifest():
    run1 = db.create_run(1)
    _seed(run1, 1, [("P1", "B1")])
    db.commit_run(run1, 1)

    run2 = db.create_run(2)
    # 故意只建公告暂存、不建批号暂存，模拟未完成采集
    db.stage_announcements(run2, 2, [])
    with pytest.raises(RuntimeError):
        db.commit_run(run2, 2)

    meta = db.get_published_meta()
    assert meta["published_run_id"] == run1
    assert len(db.list_manifest()) == 1


def test_duplicate_batch_aggregates_source_announcements():
    run = db.create_run(3)
    db.stage_announcements(run, 3, [
        {"announcement_id": "A1", "title": "t1", "published_at": None,
         "withdrawn": False, "withdraw_reason": None,
         "detail_url": "/x", "attachment_url": "/a1.csv"},
        {"announcement_id": "A2", "title": "t2", "published_at": None,
         "withdrawn": False, "withdraw_reason": None,
         "detail_url": "/x", "attachment_url": "/a2.csv"},
    ])
    db.stage_batches(run, [
        {"announcement_id": "A1", "product_code": "P1", "batch_no": "B1",
         "product_name": "名称", "produced_at": None, "reason": "r"},
        {"announcement_id": "A2", "product_code": "P1", "batch_no": "B1",
         "product_name": "名称", "produced_at": None, "reason": "r"},
        {"announcement_id": "A2", "product_code": "P1", "batch_no": "B2",
         "product_name": "名称", "produced_at": None, "reason": "r"},
    ])
    db.commit_run(run, 3)
    entries = {e["batch_no"]: e for e in db.list_manifest()}
    assert set(entries) == {"B1", "B2"}
    assert entries["B1"]["source_ann_ids"] == ["A1", "A2"]
