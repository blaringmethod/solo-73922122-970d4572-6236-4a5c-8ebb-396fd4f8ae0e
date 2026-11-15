"""本地 HTTP 演示源：模拟食品安全召回公示站。

按递增的目录版本（v1、v2、v3……）分页提供公告与 UTF-8 CSV 批号附件，
并通过 /control/* 接口支持：
  - 切换当前目录版本；
  - 撤回 / 恢复公告（运行时覆盖静态数据）；
  - 模拟附件故障（HTTP 500 或损坏字节）；
  - 注入响应延时（便于模拟请求中断）。

所有响应均通过 X-Directory-Version 头声明当前目录版本，
采集端据此检测“翻页过程中版本发生变化”。
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("FEED_DATA_DIR", BASE_DIR / "data"))
TEMPLATES_DIR = BASE_DIR / "templates"

VALID_FAULT_MODES = {"none", "error", "corrupt"}


def _load_catalog() -> tuple[list[int], dict[int, list[dict]], dict[int, Path]]:
    """扫描数据目录，建立 版本 -> 分页/附件目录 的索引。"""
    pages: dict[int, list[dict]] = {}
    attach_dirs: dict[int, Path] = {}
    versions: list[int] = []
    for child in sorted(DATA_DIR.iterdir()):
        match = re.fullmatch(r"v(\d+)", child.name)
        if not match or not child.is_dir():
            continue
        version = int(match.group(1))
        page_dir = child / "pages"
        page_files = sorted(
            page_dir.glob("page_*.json"),
            key=lambda p: int(re.search(r"page_(\d+)", p.stem).group(1)),
        )
        pages[version] = [json.loads(p.read_text(encoding="utf-8")) for p in page_files]
        attach_dirs[version] = child / "attachments"
        versions.append(version)
    if not versions:
        raise RuntimeError(f"数据目录 {DATA_DIR} 下没有任何 v* 版本目录")
    return sorted(versions), pages, attach_dirs


VERSIONS, VERSION_PAGES, ATTACH_DIRS = _load_catalog()


@dataclass
class FeedState:
    """演示源的可变运行状态（进程内，reset 可恢复初始值）。"""

    version: int = VERSIONS[0]
    # 运行时撤回覆盖：announcement_id -> {"reason": ...}
    withdrawn: dict[str, dict] = field(default_factory=dict)
    # 附件故障注入：文件名 -> error / corrupt
    attachment_faults: dict[str, str] = field(default_factory=dict)
    # 每个响应前注入的延时（秒），用于模拟慢响应 / 请求中断
    delay_seconds: float = 0.0
    # 状态变更审计
    events: list[dict] = field(default_factory=list)
    # 请求计数（便于测试确定性等待采集端开始抓取）
    request_counts: dict[str, int] = field(default_factory=dict)

    def log(self, action: str, detail: dict) -> None:
        self.events.append({"action": action, "detail": detail})
        self.events[:] = self.events[-50:]

    def count_request(self, key: str) -> None:
        self.request_counts[key] = self.request_counts.get(key, 0) + 1


STATE = FeedState()
STATE_LOCK = asyncio.Lock()
app = FastAPI(title="召回公示演示源", version="1.0.0")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _announcement_index(version: int) -> dict[str, dict]:
    index: dict[str, dict] = {}
    for page in VERSION_PAGES[version]:
        for ann in page["announcements"]:
            index[ann["announcement_id"]] = dict(ann)
    return index


def _effective_announcement(ann: dict) -> dict:
    """叠加运行时撤回覆盖后的公告视图。"""
    merged = dict(ann)
    override = STATE.withdrawn.get(ann["announcement_id"])
    if override is not None:
        merged["withdrawn"] = True
        merged["withdraw_reason"] = override.get("reason", "演示源运行时撤回")
        merged.setdefault("withdrawn_at", "运行时撤回")
    return merged


def _feed_headers(version: int, page: Optional[int] = None,
                  total_pages: Optional[int] = None) -> dict[str, str]:
    headers = {"X-Directory-Version": str(version)}
    if page is not None:
        headers["X-Feed-Page"] = str(page)
    if total_pages is not None:
        headers["X-Feed-Total-Pages"] = str(total_pages)
    return headers


@app.get("/", response_class=HTMLResponse)
async def control_panel(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "versions": VERSIONS,
            "announcements": _announcement_index(STATE.version),
        },
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": STATE.version, "versions": VERSIONS}


@app.get("/feed")
async def list_announcements(request: Request, page: int = 1) -> JSONResponse:
    """公告分页列表。"""
    STATE.count_request("feed")
    version = STATE.version  # 请求到达即定版，延时期间切换不影响本响应
    if STATE.delay_seconds:
        await asyncio.sleep(STATE.delay_seconds)
    pages = VERSION_PAGES[version]
    if page < 1 or page > len(pages):
        raise HTTPException(status_code=404, detail=f"页码 {page} 不存在")
    payload = dict(pages[page - 1])
    payload["directory_version"] = version
    payload["announcements"] = [
        _effective_announcement(a) for a in payload["announcements"]
    ]
    return JSONResponse(
        payload,
        headers=_feed_headers(version, payload["page"], payload["total_pages"]),
    )


@app.get("/feed/announcements/{announcement_id}")
async def get_announcement(announcement_id: str) -> JSONResponse:
    version = STATE.version
    if STATE.delay_seconds:
        await asyncio.sleep(STATE.delay_seconds)
    index = _announcement_index(version)
    if announcement_id not in index:
        raise HTTPException(status_code=404, detail="当前版本中不存在该公告")
    body = _effective_announcement(index[announcement_id])
    body["directory_version"] = version
    return JSONResponse(body, headers=_feed_headers(version))


@app.get("/feed/attachments/{filename}")
async def get_attachment(filename: str) -> Response:
    STATE.count_request("attachment")
    version = STATE.version  # 请求到达即定版
    if STATE.delay_seconds:
        await asyncio.sleep(STATE.delay_seconds)
    attach_dir = ATTACH_DIRS[version]
    # 防路径穿越
    if "/" in filename or ".." in filename or not filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="非法附件名")
    path = attach_dir / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"附件 {filename} 不存在")

    fault = STATE.attachment_faults.get(filename)
    if fault == "error":
        raise HTTPException(status_code=500, detail=f"模拟附件服务器故障：{filename}")

    raw = path.read_bytes()
    if fault == "corrupt":
        # 注入非法 UTF-8 字节并截断，采集端严格解码必然失败
        raw = b"\xff\xfe" + raw[: max(0, len(raw) // 2)]
    return Response(
        content=raw,
        media_type="text/csv; charset=utf-8",
        headers={
            **_feed_headers(version),
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


# ---------------------------------------------------------------------------
# 控制面板接口
# ---------------------------------------------------------------------------
@app.get("/control/state")
async def get_state() -> dict:
    return {
        "version": STATE.version,
        "versions": VERSIONS,
        "withdrawn": STATE.withdrawn,
        "attachment_faults": STATE.attachment_faults,
        "delay_seconds": STATE.delay_seconds,
        "request_counts": dict(STATE.request_counts),
        "announcements": [
            _effective_announcement(a)
            for a in _announcement_index(STATE.version).values()
        ],
        "events": list(reversed(STATE.events)),
    }


@app.post("/control/version")
async def set_version(body: dict) -> dict:
    version = int(body.get("version", 0))
    if version not in VERSIONS:
        raise HTTPException(status_code=400, detail=f"未知版本 {version}")
    async with STATE_LOCK:
        old = STATE.version
        STATE.version = version
        STATE.log("switch_version", {"from": old, "to": version})
    return {"ok": True, "previous_version": old, "version": version}


@app.post("/control/withdraw")
async def withdraw_announcement(body: dict) -> dict:
    announcement_id = str(body.get("announcement_id", "")).strip()
    index = _announcement_index(STATE.version)
    if announcement_id not in index:
        raise HTTPException(status_code=404, detail="当前版本中不存在该公告")
    reason = str(body.get("reason", "演示源运行时撤回"))
    async with STATE_LOCK:
        STATE.withdrawn[announcement_id] = {"reason": reason}
        STATE.log("withdraw", {"announcement_id": announcement_id, "reason": reason})
    return {"ok": True, "announcement_id": announcement_id}


@app.post("/control/restore")
async def restore_announcement(body: dict) -> dict:
    announcement_id = str(body.get("announcement_id", "")).strip()
    async with STATE_LOCK:
        existed = STATE.withdrawn.pop(announcement_id, None)
        STATE.log("restore", {"announcement_id": announcement_id})
    if existed is None:
        raise HTTPException(status_code=404, detail="该公告没有运行时撤回覆盖")
    return {"ok": True, "announcement_id": announcement_id}


@app.post("/control/fault")
async def set_fault(body: dict) -> dict:
    filename = str(body.get("attachment", "")).strip()
    mode = str(body.get("mode", "none")).strip()
    if mode not in VALID_FAULT_MODES:
        raise HTTPException(status_code=400, detail=f"mode 必须是 {sorted(VALID_FAULT_MODES)}")
    async with STATE_LOCK:
        if mode == "none":
            STATE.attachment_faults.pop(filename, None)
        else:
            STATE.attachment_faults[filename] = mode
        STATE.log("fault", {"attachment": filename, "mode": mode})
    return {"ok": True, "attachment": filename, "mode": mode}


@app.post("/control/delay")
async def set_delay(body: dict) -> dict:
    seconds = float(body.get("seconds", 0))
    if seconds < 0 or seconds > 60:
        raise HTTPException(status_code=400, detail="延时需在 0~60 秒之间")
    async with STATE_LOCK:
        STATE.delay_seconds = seconds
        STATE.log("delay", {"seconds": seconds})
    return {"ok": True, "delay_seconds": seconds}


@app.post("/control/reset")
async def reset_state() -> dict:
    async with STATE_LOCK:
        STATE.version = VERSIONS[0]
        STATE.withdrawn.clear()
        STATE.attachment_faults.clear()
        STATE.delay_seconds = 0.0
        STATE.events.clear()
        STATE.request_counts.clear()
        STATE.log("reset", {})
    return {"ok": True}
