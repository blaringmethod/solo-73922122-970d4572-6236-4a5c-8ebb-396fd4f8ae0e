"""食品安全召回采集端 Web 应用（FastAPI）。

页面：
  GET /                操作面板（发起/取消采集、查看运行历史与失败原因）
  GET /manifest        当前拦截清单（按产品编码、批号汇总 + 来源公告）
API：
  POST /api/collect        发起采集
  POST /api/runs/{id}/cancel  取消正在进行的采集
  GET  /api/runs           运行历史
  GET  /api/manifest       当前清单 JSON（网页同源数据）
  GET  /api/announcements  最近发布版本的公告（含撤回状态）
  GET  /stop-sale/manifest.json  停售系统读取的同版 JSON（与落盘文件一致）
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import config, db
from .collector import reason_text, run_collection

BASE_DIR = Path(__file__).resolve().parent

# 运行中的采集任务：run_id -> (asyncio.Task, cancel_event)
_RUNNING: dict[int, tuple[asyncio.Task, asyncio.Event]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield
    # 进程退出时取消未完成的采集（暂存数据会被清理，正式清单不变）
    for _, event in list(_RUNNING.values()):
        event.set()
    for task, _ in list(_RUNNING.values()):
        task.cancel()


app = FastAPI(title="食品安全召回采集端", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _run_view(row: dict) -> dict:
    row = dict(row)
    row["reason_text"] = reason_text(row.get("reason_code"))
    row["running"] = row["status"] == "running" and row["id"] in _RUNNING
    return row


async def _task_done(run_id: int, task: asyncio.Task) -> None:
    _RUNNING.pop(run_id, None)
    if task.cancelled():
        # 正常的取消路径已在 collector 内落库；此处仅兜底
        run = db.get_run(run_id)
        if run and run["status"] == "running":
            db.finish_run(run_id, "cancelled", "CANCELLED", reason_text("CANCELLED"))
            db.drop_staging(run_id)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "running": bool(_RUNNING)}


@app.post("/api/collect")
async def start_collect() -> dict:
    if config.SINGLE_RUN_GUARD and _RUNNING:
        raise HTTPException(status_code=409, detail="已有采集任务在运行，请等待或取消后再试")
    run_id = db.create_run(target_version=-1)
    cancel_event = asyncio.Event()
    task = asyncio.create_task(run_collection(run_id, cancel_event))
    _RUNNING[run_id] = (task, cancel_event)
    task.add_done_callback(lambda t: asyncio.create_task(_task_done(run_id, t)))
    return {"run_id": run_id, "status": "running"}


@app.post("/api/runs/{run_id}/cancel")
async def cancel_run(run_id: int) -> dict:
    pair = _RUNNING.get(run_id)
    if pair is None:
        raise HTTPException(status_code=404, detail="该采集不在运行中，无法取消")
    _, event = pair
    event.set()
    return {"ok": True, "run_id": run_id}


@app.get("/api/runs")
async def api_runs(limit: int = 20) -> list[dict]:
    return [_run_view(r) for r in db.list_runs(limit)]


@app.get("/api/manifest")
async def api_manifest(product_code: Optional[str] = Query(default=None)) -> dict:
    entries = db.list_manifest(product_code)
    meta = db.get_published_meta()
    return {
        "meta": meta,
        "entries": entries,
        "entry_count": len(entries),
    }


@app.get("/api/announcements")
async def api_announcements() -> list[dict]:
    return db.list_announcements()


@app.get("/api/announcements/{announcement_id}")
async def api_announcement_detail(announcement_id: str) -> dict:
    row = db.get_announcement(announcement_id)
    if row is None:
        raise HTTPException(status_code=404, detail="当前清单版本中没有该公告")
    return row


@app.get("/stop-sale/manifest.json")
async def stop_sale_manifest() -> JSONResponse:
    """停售系统读取入口：优先返回与数据库同版的落盘 JSON。"""
    path = config.MANIFEST_PATH
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return JSONResponse(data)
        except (json.JSONDecodeError, OSError):
            pass
    meta = db.get_published_meta()
    if meta is None:
        return JSONResponse(
            {"manifest_version": 1, "entry_count": 0, "products": [], "note": "尚未发布任何清单"}
        )
    raise HTTPException(status_code=500, detail="清单文件暂不可读，请稍后重试")


# ---------------------------------------------------------------------------
# 页面
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "feed_base_url": config.FEED_BASE_URL,
            "manifest_path": str(config.MANIFEST_PATH),
        },
    )


@app.get("/manifest", response_class=HTMLResponse)
async def page_manifest(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "manifest.html", {})
