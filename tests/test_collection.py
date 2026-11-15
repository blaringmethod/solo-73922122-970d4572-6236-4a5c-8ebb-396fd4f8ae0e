"""关键保证的端到端测试。"""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from conftest import (
    batch_keys,
    manifest,
    start_run,
    stop_sale_json,
    wait_feed_requests,
    wait_run,
)


# ---------------------------------------------------------------------------
# 1. 全量采集：读全分页与有效附件，整体替换并发布同版 JSON
# ---------------------------------------------------------------------------
def test_full_collect_v1_publishes_manifest(urls):
    feed_url, app_url = urls
    run = wait_run(app_url, start_run(app_url))

    assert run["status"] == "success", run
    assert run["source_version"] == 1
    assert run["pages_read"] == 2          # v1 共两页
    assert run["attachments_read"] == 3    # R001/R002/R003 全部有效
    assert run["batches_collected"] == 7

    keys = batch_keys(app_url)
    assert ("SP-NY-001", "B20261020A") in keys
    assert ("SP-GJ-101", "B20261018N") in keys
    assert ("SP-YS-210", "B20261025S") in keys
    assert len(keys) == 7

    meta = manifest(app_url)["meta"]
    assert meta["source_version"] == 1

    # 停售 JSON 与数据库同版
    payload = stop_sale_json(app_url)
    assert payload["source_directory_version"] == 1
    assert payload["published_run_id"] == run["id"]
    assert payload["entry_count"] == 7
    flat = {(p["product_code"], b["batch_no"])
            for p in payload["products"] for b in p["batches"]}
    assert flat == keys


# ---------------------------------------------------------------------------
# 2. 新版本采集：撤回公告解除其独有批号；被其他有效公告覆盖的批号保留
# ---------------------------------------------------------------------------
def test_v2_withdraw_drops_only_uncovered_batches(urls):
    feed_url, app_url = urls
    first = wait_run(app_url, start_run(app_url))
    assert first["status"] == "success"

    httpx.post(f"{feed_url}/control/version", json={"version": 2}, timeout=5)
    second = wait_run(app_url, start_run(app_url))
    assert second["status"] == "success"
    assert second["source_version"] == 2
    assert second["attachments_read"] == 3  # R001 已撤回，只读 R002/R003/R004

    keys = batch_keys(app_url)
    # R001 独有批号随撤回解除
    assert ("SP-NY-001", "B20261020A") not in keys
    assert ("SP-NY-001", "B20261021A") not in keys
    assert ("SP-NY-002", "B20261022C") not in keys
    # 其他有效公告覆盖的批号仍在拦截
    assert ("SP-GJ-101", "B20261018N") in keys
    assert ("SP-YS-210", "B20261025S") in keys
    # 新增 R004 批号进入清单
    assert ("SP-GC-300", "B20261028H") in keys

    announcements = httpx.get(f"{app_url}/api/announcements", timeout=5).json()
    r001 = next(a for a in announcements if a["announcement_id"] == "R001")
    assert r001["withdrawn"] == 1


def test_withdraw_one_announcement_keeps_batches_covered_by_other(urls):
    """v3：R003 撤回，但其 B20261026S 仍被有效公告 R005 覆盖，不能解除拦截。"""
    feed_url, app_url = urls
    wait_run(app_url, start_run(app_url))  # v1
    httpx.post(f"{feed_url}/control/version", json={"version": 3}, timeout=5)
    run = wait_run(app_url, start_run(app_url))
    assert run["status"] == "success"
    assert run["pages_read"] == 3
    assert run["attachments_read"] == 3  # R002/R004/R005

    keys = batch_keys(app_url)
    # R003 独有批号解除
    assert ("SP-YS-210", "B20261025S") not in keys
    # R005 同时覆盖的批号继续拦截，且来源聚合为 R005
    assert ("SP-YS-210", "B20261026S") in keys
    entry = next(e for e in manifest(app_url)["entries"]
                 if e["product_code"] == "SP-YS-210" and e["batch_no"] == "B20261026S")
    assert entry["source_ann_ids"] == ["R005"]
    # R005 新增批号
    assert ("SP-YS-210", "B20261101S") in keys


def test_runtime_withdraw_same_version(urls):
    """同版本运行时撤回（不切版本），重新采集后清单即时更新。"""
    feed_url, app_url = urls
    wait_run(app_url, start_run(app_url))
    httpx.post(f"{feed_url}/control/withdraw",
               json={"announcement_id": "R002", "reason": "测试撤回"}, timeout=5)
    run = wait_run(app_url, start_run(app_url))
    assert run["status"] == "success"
    assert run["source_version"] == 1
    keys = batch_keys(app_url)
    assert ("SP-GJ-101", "B20261018N") not in keys
    assert ("SP-GJ-101", "B20261019N") not in keys
    assert ("SP-NY-001", "B20261020A") in keys  # 其他公告不受影响


# ---------------------------------------------------------------------------
# 3. 附件故障：保留上轮清单并记录原因
# ---------------------------------------------------------------------------
def test_attachment_http_error_keeps_previous_manifest(urls):
    feed_url, app_url = urls
    wait_run(app_url, start_run(app_url))
    before = batch_keys(app_url)

    httpx.post(f"{feed_url}/control/fault",
               json={"attachment": "R002_batches.csv", "mode": "error"}, timeout=5)
    run = wait_run(app_url, start_run(app_url))
    assert run["status"] == "failed"
    assert run["reason_code"] == "ATTACHMENT_HTTP_ERROR"
    assert batch_keys(app_url) == before  # 上轮清单原样保留


def test_attachment_corrupt_keeps_previous_manifest(urls):
    feed_url, app_url = urls
    wait_run(app_url, start_run(app_url))
    before = batch_keys(app_url)

    httpx.post(f"{feed_url}/control/fault",
               json={"attachment": "R003_batches.csv", "mode": "corrupt"}, timeout=5)
    run = wait_run(app_url, start_run(app_url))
    assert run["status"] == "failed"
    assert run["reason_code"] == "ATTACHMENT_CORRUPT"
    assert batch_keys(app_url) == before

    # 故障恢复后采集成功
    httpx.post(f"{feed_url}/control/fault",
               json={"attachment": "R003_batches.csv", "mode": "none"}, timeout=5)
    ok = wait_run(app_url, start_run(app_url))
    assert ok["status"] == "success"
    assert batch_keys(app_url) == before


# ---------------------------------------------------------------------------
# 4. 翻页过程中版本变化：中止并保留上轮清单
# ---------------------------------------------------------------------------
def test_version_change_mid_pagination(urls):
    feed_url, app_url = urls
    # 先在 v2 上成功发布（v2 高于失败采集钉住的 v1，避免触发旧版本保护）
    httpx.post(f"{feed_url}/control/version", json={"version": 2}, timeout=5)
    wait_run(app_url, start_run(app_url))
    before = batch_keys(app_url)

    # v1 + 大延时发起采集，确认第一页已被读取（已钉住 v1）后再切到 v3
    httpx.post(f"{feed_url}/control/version", json={"version": 1}, timeout=5)
    httpx.post(f"{feed_url}/control/delay", json={"seconds": 1}, timeout=5)
    run_id = start_run(app_url)
    wait_feed_requests(feed_url, "feed", minimum=1)
    httpx.post(f"{feed_url}/control/version", json={"version": 3}, timeout=5)
    run = wait_run(app_url, run_id)
    httpx.post(f"{feed_url}/control/delay", json={"seconds": 0}, timeout=5)

    assert run["status"] == "failed"
    assert run["reason_code"] == "VERSION_CHANGED"
    assert batch_keys(app_url) == before
    assert manifest(app_url)["meta"]["source_version"] == 2


# ---------------------------------------------------------------------------
# 5. 手动取消（请求中断）：保留上轮清单
# ---------------------------------------------------------------------------
def test_cancel_keeps_previous_manifest(urls):
    feed_url, app_url = urls
    wait_run(app_url, start_run(app_url))
    before = batch_keys(app_url)

    httpx.post(f"{feed_url}/control/delay", json={"seconds": 3}, timeout=5)
    run_id = start_run(app_url)
    # 等采集真正进入请求阶段后取消
    import time
    time.sleep(1.0)
    resp = httpx.post(f"{app_url}/api/runs/{run_id}/cancel", timeout=5)
    assert resp.status_code == 200
    run = wait_run(app_url, run_id)
    httpx.post(f"{feed_url}/control/delay", json={"seconds": 0}, timeout=5)

    assert run["status"] == "cancelled"
    assert run["reason_code"] == "CANCELLED"
    assert batch_keys(app_url) == before


# ---------------------------------------------------------------------------
# 6. 并发采集：旧版本晚完成也不能覆盖先发布的新版本
# ---------------------------------------------------------------------------
def test_concurrent_stale_version_cannot_overwrite(urls):
    feed_url, app_url = urls
    from app import db
    from app.collector import run_collection

    # 先发布 v2 清单
    httpx.post(f"{feed_url}/control/version", json={"version": 2}, timeout=5)
    published = wait_run(app_url, start_run(app_url))
    assert published["status"] == "success"
    v2_keys = batch_keys(app_url)

    async def scenario():
        # 演示源切到 v3，新运行先成功发布
        httpx.post(f"{feed_url}/control/version", json={"version": 3}, timeout=5)
        new_run_id = db.create_run(target_version=3)
        await run_collection(new_run_id, asyncio.Event())
        assert db.get_run(new_run_id)["status"] == "success"

        # 演示源回到 v1：迟到的旧版本运行尝试提交，必须被旧版本保护拒绝
        httpx.post(f"{feed_url}/control/version", json={"version": 1}, timeout=5)
        stale_run_id = db.create_run(target_version=1)
        await run_collection(stale_run_id, asyncio.Event())
        return new_run_id, stale_run_id

    new_run_id, stale_run_id = await_scenario(scenario())

    stale = db.get_run(stale_run_id)
    assert stale["status"] == "stale"
    assert stale["reason_code"] == "STALE_VERSION"
    # 正式清单仍是先发布的新版本
    assert manifest(app_url)["meta"]["source_version"] == 3
    assert batch_keys(app_url) != v2_keys  # v3 已生效
    assert ("SP-YS-210", "B20261101S") in batch_keys(app_url)

    # 落盘 JSON 同样没有被旧版本回退
    payload = stop_sale_json(app_url)
    assert payload["source_directory_version"] == 3


def test_api_rejects_parallel_collect(urls):
    """默认单运行保护：第二个并发请求得到 409。"""
    feed_url, app_url = urls
    httpx.post(f"{feed_url}/control/delay", json={"seconds": 2}, timeout=5)
    first = httpx.post(f"{app_url}/api/collect", timeout=10)
    assert first.status_code == 200
    second = httpx.post(f"{app_url}/api/collect", timeout=10)
    assert second.status_code == 409
    run_id = first.json()["run_id"]
    run = wait_run(app_url, run_id)
    httpx.post(f"{feed_url}/control/delay", json={"seconds": 0}, timeout=5)
    assert run["status"] == "success"


# ---------------------------------------------------------------------------
# 7. 页面与原始数据形态
# ---------------------------------------------------------------------------
def test_pages_render(urls):
    _, app_url = urls
    for path in ("/", "/manifest"):
        resp = httpx.get(f"{app_url}{path}", timeout=10)
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]


def test_feed_serves_utf8_chinese_csv(urls):
    feed_url, _ = urls
    resp = httpx.get(f"{feed_url}/feed/attachments/R001_batches.csv", timeout=5)
    assert resp.status_code == 200
    text = resp.content.decode("utf-8")
    assert "鲜牛乳" in text
    assert resp.headers["X-Directory-Version"] == "1"


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------
def await_scenario(coro):
    """在同步 pytest 中驱动协程直至结束。"""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# 兼容命名
await_task = await_scenario
