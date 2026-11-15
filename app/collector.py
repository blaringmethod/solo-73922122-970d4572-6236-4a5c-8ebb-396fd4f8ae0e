"""采集器：从演示源读全同一目录版本的所有分页与有效公告附件。

保证：
1. 第一次请求即“钉住”目录版本，后续任何响应头版本不一致立刻中止
   （模拟翻页过程中源站切换版本）；
2. 所有分页、所有有效公告的附件全部成功读取并解析后才允许发布，
   任一环节失败都保留上一轮清单并记录原因；
3. 支持外部取消（请求中断），取消后只落暂存垃圾、不动正式清单；
4. 发布阶段由 db.commit_run 做旧版本保护，并发采集的旧版本运行
   即使晚完成也无法覆盖先发布的新版本。
"""
from __future__ import annotations

import asyncio
import csv
import io
from dataclasses import dataclass, field
from typing import Optional

import httpx

from . import config, db

# 原因码 -> 中文说明（页面与 API 共用）
REASON_TEXTS = {
    "FEED_UNREACHABLE": "无法连接召回公示源",
    "FEED_HTTP_ERROR": "公示源返回错误状态码",
    "VERSION_CHANGED": "采集过程中目录版本发生变化，已中止并保留上轮清单",
    "PAGE_INCOMPLETE": "分页数据不完整或分页链断裂",
    "ATTACHMENT_HTTP_ERROR": "附件下载失败（源站故障）",
    "ATTACHMENT_CORRUPT": "附件损坏或不是合法 UTF-8 CSV",
    "ATTACHMENT_BAD_FORMAT": "附件缺少必需列或批号行为空",
    "CANCELLED": "采集被手动取消（请求中断），已保留上轮清单",
    "STALE_VERSION": "旧版本采集结果晚于新版本发布，已拒绝覆盖",
    "UNEXPECTED": "采集发生未预期错误",
}

REQUIRED_COLUMNS = {"product_code", "product_name", "batch_no"}


@dataclass
class CrawlResult:
    source_version: int
    total_pages: int
    announcements: list[dict] = field(default_factory=list)
    batch_rows: list[dict] = field(default_factory=list)
    attachments_read: int = 0


class CrawlError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def reason_text(code: Optional[str]) -> str:
    if not code:
        return ""
    return REASON_TEXTS.get(code, code)


def _check_version(pinned: int, response: httpx.Response) -> None:
    header = response.headers.get("X-Directory-Version")
    try:
        current = int(header)
    except (TypeError, ValueError):
        raise CrawlError(
            "VERSION_CHANGED",
            f"响应缺少合法的 X-Directory-Version 头（实际值：{header!r}）",
        )
    if current != pinned:
        raise CrawlError(
            "VERSION_CHANGED",
            f"采集钉住版本 v{pinned}，但响应来自 v{current}，"
            "检测到翻页过程中源站切换目录版本",
        )


async def _get(client: httpx.AsyncClient, url: str, *, pinned: Optional[int],
               expect_json: bool) -> httpx.Response:
    try:
        resp = await client.get(url)
    except httpx.HTTPError as exc:
        if isinstance(exc, httpx.TimeoutException):
            raise CrawlError("FEED_UNREACHABLE", f"请求超时：{url}（{exc}）")
        raise CrawlError("FEED_UNREACHABLE", f"请求失败：{url}（{exc}）")
    if resp.status_code != 200:
        code = "ATTACHMENT_HTTP_ERROR" if not expect_json else "FEED_HTTP_ERROR"
        raise CrawlError(code, f"{url} 返回 HTTP {resp.status_code}")
    if pinned is not None:
        _check_version(pinned, resp)
    return resp


def _parse_attachment(announcement_id: str, raw: bytes) -> list[dict]:
    """严格按 UTF-8 解析 CSV；任何编码或结构问题都视为附件损坏。"""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CrawlError(
            "ATTACHMENT_CORRUPT",
            f"公告 {announcement_id} 的附件不是合法 UTF-8：{exc}",
        )
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None or not REQUIRED_COLUMNS.issubset(set(reader.fieldnames)):
        raise CrawlError(
            "ATTACHMENT_BAD_FORMAT",
            f"公告 {announcement_id} 的附件缺少必需列 "
            f"{sorted(REQUIRED_COLUMNS)}，实际列：{reader.fieldnames}",
        )
    rows: list[dict] = []
    for line_no, row in enumerate(reader, start=2):
        product_code = (row.get("product_code") or "").strip()
        batch_no = (row.get("batch_no") or "").strip()
        product_name = (row.get("product_name") or "").strip()
        if not product_code or not batch_no:
            raise CrawlError(
                "ATTACHMENT_BAD_FORMAT",
                f"公告 {announcement_id} 附件第 {line_no} 行产品编码或批号为空",
            )
        rows.append(
            {
                "announcement_id": announcement_id,
                "product_code": product_code,
                "batch_no": batch_no,
                "product_name": product_name or product_code,
                "produced_at": (row.get("produced_at") or "").strip() or None,
                "reason": (row.get("reason") or "").strip() or None,
            }
        )
    if not rows:
        raise CrawlError(
            "ATTACHMENT_BAD_FORMAT", f"公告 {announcement_id} 的附件没有任何批号行"
        )
    return rows


async def crawl(cancel_event: asyncio.Event) -> CrawlResult:
    """执行一次完整采集；失败抛 CrawlError，取消抛 asyncio.CancelledError。"""
    timeout = httpx.Timeout(config.HTTP_TIMEOUT, connect=config.HTTP_TIMEOUT)
    async with httpx.AsyncClient(base_url=config.FEED_BASE_URL, timeout=timeout) as client:
        # ---- 第一页：钉住目录版本 ----
        first = await _get(client, "/feed?page=1", pinned=None, expect_json=True)
        try:
            pinned = int(first.headers["X-Directory-Version"])
            page1 = first.json()
        except (KeyError, TypeError, ValueError) as exc:
            raise CrawlError("VERSION_CHANGED", f"首页版本头无法解析：{exc}")

        result = CrawlResult(source_version=pinned, total_pages=int(page1["total_pages"]))
        if page1.get("page") != 1 or result.total_pages < 1:
            raise CrawlError("PAGE_INCOMPLETE", "首页分页字段异常")
        pages = [page1]

        # ---- 顺序读完同一版本的所有分页 ----
        expected_next = page1.get("next_page")
        for expected_page in range(2, result.total_pages + 1):
            if cancel_event.is_set():
                raise asyncio.CancelledError()
            if expected_next != expected_page:
                raise CrawlError(
                    "PAGE_INCOMPLETE",
                    f"分页链断裂：期望第 {expected_page} 页，next_page={expected_next}",
                )
            resp = await _get(
                client, f"/feed?page={expected_page}", pinned=pinned, expect_json=True
            )
            body = resp.json()
            if body.get("page") != expected_page or body.get("total_pages") != result.total_pages:
                raise CrawlError(
                    "PAGE_INCOMPLETE",
                    f"第 {expected_page} 页分页字段与首页不一致",
                )
            pages.append(body)
            expected_next = body.get("next_page")
        if expected_next is not None:
            raise CrawlError("PAGE_INCOMPLETE", "最后一页 next_page 应为 null")

        for page in pages:
            result.announcements.extend(page["announcements"])
        if not result.announcements:
            raise CrawlError("PAGE_INCOMPLETE", "所有分页均无公告数据")

        # 公告号唯一性检查
        ids = [a["announcement_id"] for a in result.announcements]
        if len(ids) != len(set(ids)):
            raise CrawlError("PAGE_INCOMPLETE", f"公告号出现重复：{ids}")

        # ---- 读取所有“有效公告”的附件；已撤回公告不参与拦截 ----
        active = [a for a in result.announcements if not a.get("withdrawn")]
        for ann in active:
            if cancel_event.is_set():
                raise asyncio.CancelledError()
            attachment = ann.get("attachment_url")
            if not attachment:
                raise CrawlError(
                    "ATTACHMENT_BAD_FORMAT",
                    f"有效公告 {ann['announcement_id']} 缺少附件地址",
                )
            resp = await _get(client, attachment, pinned=pinned, expect_json=False)
            rows = _parse_attachment(ann["announcement_id"], resp.content)
            result.batch_rows.extend(rows)
            result.attachments_read += 1

        return result


# ---------------------------------------------------------------------------
# 运行编排
# ---------------------------------------------------------------------------
async def run_collection(run_id: int, cancel_event: asyncio.Event) -> None:
    """完整生命周期：采集 -> 暂存 -> 原子发布；任何失败只记录原因。"""
    try:
        result = await crawl(cancel_event)
        if cancel_event.is_set():
            raise asyncio.CancelledError()

        # 抓取完成后先落暂存表（阻塞型 SQLite 调用放到线程池）
        await asyncio.to_thread(db.stage_announcements, run_id,
                                result.source_version, result.announcements)
        await asyncio.to_thread(db.stage_batches, run_id, result.batch_rows)
        await asyncio.to_thread(
            db.update_run_counters, run_id, result.total_pages,
            result.attachments_read, len(result.batch_rows), result.source_version,
        )

        try:
            await asyncio.to_thread(db.commit_run, run_id, result.source_version)
        except db.StalePublishError as exc:
            await asyncio.to_thread(db.finish_run, run_id, "stale",
                                    "STALE_VERSION", str(exc))
            await asyncio.to_thread(db.drop_staging, run_id)
            return
    except asyncio.CancelledError:
        await asyncio.to_thread(db.finish_run, run_id, "cancelled",
                                "CANCELLED", reason_text("CANCELLED"))
        await asyncio.to_thread(db.drop_staging, run_id)
        raise
    except CrawlError as exc:
        await asyncio.to_thread(db.finish_run, run_id, "failed",
                                exc.code, exc.message)
        await asyncio.to_thread(db.drop_staging, run_id)
    except Exception as exc:  # noqa: BLE001 - 兜底，确保正式清单不受影响
        await asyncio.to_thread(db.finish_run, run_id, "failed",
                                "UNEXPECTED", str(exc), repr(exc))
        await asyncio.to_thread(db.drop_staging, run_id)
