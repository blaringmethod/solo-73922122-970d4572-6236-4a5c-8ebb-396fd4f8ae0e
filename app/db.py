"""SQLite 持久层。

设计要点：
- 每个采集运行（run）先把抓取结果写入以 run_id 命名的 *暂存表*，
  成功读完全部分页与有效附件后，在单个事务内整体替换正式清单；
- 正式清单表 manifest_entries 带部分唯一索引，保证
  (产品编码, 批号) 只有一条有效记录，来源公告为多值聚合；
- 提交时做“旧版本保护”：只有更新的源版本或更大的 run_id 才能发布，
  并发采集里晚完成的旧版本运行无法覆盖先发布的新版本；
- 清单 JSON 文件与数据库提交保持同版（先原子写文件，再提交事务）。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from . import config

# 写操作串行化（SQLite 单写者），配合 WAL 提升并发读体验
_WRITE_LOCK = threading.RLock()

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS collection_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    status            TEXT NOT NULL CHECK (status IN ('running','success','failed','cancelled','stale')),
    source_version    INTEGER,
    target_version    INTEGER NOT NULL,
    pages_read        INTEGER NOT NULL DEFAULT 0,
    attachments_read  INTEGER NOT NULL DEFAULT 0,
    batches_collected INTEGER NOT NULL DEFAULT 0,
    reason_code       TEXT,
    reason_message    TEXT,
    error_detail      TEXT
);

CREATE TABLE IF NOT EXISTS source_announcements (
    run_id             INTEGER NOT NULL REFERENCES collection_runs(id),
    source_version     INTEGER NOT NULL,
    announcement_id    TEXT NOT NULL,
    title              TEXT NOT NULL,
    published_at       TEXT,
    withdrawn          INTEGER NOT NULL,
    withdraw_reason    TEXT,
    detail_url         TEXT,
    attachment_url     TEXT,
    PRIMARY KEY (run_id, announcement_id)
);

CREATE TABLE IF NOT EXISTS manifest_entries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    product_code    TEXT NOT NULL,
    batch_no        TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    produced_at     TEXT,
    reason          TEXT,
    source_ann_ids  TEXT NOT NULL,
    source_version  INTEGER NOT NULL,
    first_seen_run  INTEGER NOT NULL,
    last_updated_run INTEGER NOT NULL,
    is_current      INTEGER NOT NULL DEFAULT 1
);

-- 有效清单中 (产品编码, 批号) 全局唯一
CREATE UNIQUE INDEX IF NOT EXISTS ux_manifest_current
    ON manifest_entries(product_code, batch_no) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS ix_manifest_product
    ON manifest_entries(product_code) WHERE is_current = 1;

CREATE TABLE IF NOT EXISTS manifest_meta (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    published_run_id INTEGER NOT NULL,
    source_version   INTEGER NOT NULL,
    published_at     TEXT NOT NULL,
    entry_count      INTEGER NOT NULL
);
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect() -> sqlite3.Connection:
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(config.DB_PATH), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_db() -> None:
    with _WRITE_LOCK, connect() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# 采集运行
# ---------------------------------------------------------------------------
def create_run(target_version: Optional[int]) -> int:
    with _WRITE_LOCK, connect() as conn:
        cur = conn.execute(
            "INSERT INTO collection_runs (started_at, status, target_version) "
            "VALUES (?, 'running', ?)",
            (utc_now_iso(), target_version if target_version is not None else -1),
        )
        return int(cur.lastrowid)


def update_run_counters(run_id: int, pages: int, attachments: int, batches: int,
                        source_version: int) -> None:
    with _WRITE_LOCK, connect() as conn:
        conn.execute(
            "UPDATE collection_runs SET pages_read=?, attachments_read=?, "
            "batches_collected=?, source_version=? WHERE id=?",
            (pages, attachments, batches, source_version, run_id),
        )


def finish_run(run_id: int, status: str, reason_code: Optional[str] = None,
               reason_message: Optional[str] = None,
               error_detail: Optional[str] = None) -> None:
    with _WRITE_LOCK, connect() as conn:
        conn.execute(
            "UPDATE collection_runs SET finished_at=?, status=?, reason_code=?, "
            "reason_message=?, error_detail=? WHERE id=?",
            (utc_now_iso(), status, reason_code, reason_message, error_detail, run_id),
        )


def get_run(run_id: int) -> Optional[dict]:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM collection_runs WHERE id=?", (run_id,)
        ).fetchone()
        return dict(row) if row else None


def list_runs(limit: int = 20) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM collection_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_published_meta() -> Optional[dict]:
    with connect() as conn:
        row = conn.execute("SELECT * FROM manifest_meta WHERE id=1").fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# 暂存数据（抓取阶段写入，提交后删除）
# ---------------------------------------------------------------------------
def stage_announcements(run_id: int, source_version: int,
                        announcements: list[dict]) -> None:
    """写入本次运行看到的全部公告（含已撤回，用于来源追溯）。"""
    with _WRITE_LOCK, connect() as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS stage_ann_{run_id} ( "
            "announcement_id TEXT PRIMARY KEY, title TEXT, published_at TEXT, "
            "withdrawn INTEGER, withdraw_reason TEXT, detail_url TEXT, attachment_url TEXT)",
        )
        conn.executemany(
            f"INSERT OR REPLACE INTO stage_ann_{run_id} "
            "(announcement_id, title, published_at, withdrawn, withdraw_reason, "
            "detail_url, attachment_url) VALUES (?,?,?,?,?,?,?)",
            [
                (
                    a["announcement_id"],
                    a["title"],
                    a.get("published_at"),
                    1 if a.get("withdrawn") else 0,
                    a.get("withdraw_reason"),
                    a.get("detail_url"),
                    a.get("attachment_url"),
                )
                for a in announcements
            ],
        )


def stage_batches(run_id: int, rows: list[dict]) -> None:
    """写入从有效公告附件解析出的批号行（去重由提交阶段聚合处理）。"""
    with _WRITE_LOCK, connect() as conn:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS stage_batch_{run_id} ( "
            "product_code TEXT, batch_no TEXT, product_name TEXT, produced_at TEXT, "
            "reason TEXT, announcement_id TEXT)",
        )
        conn.executemany(
            f"INSERT INTO stage_batch_{run_id} "
            "(product_code, batch_no, product_name, produced_at, reason, announcement_id) "
            "VALUES (?,?,?,?,?,?)",
            [
                (
                    r["product_code"], r["batch_no"], r["product_name"],
                    r.get("produced_at"), r.get("reason"), r["announcement_id"],
                )
                for r in rows
            ],
        )


def drop_staging(run_id: int) -> None:
    with _WRITE_LOCK, connect() as conn:
        conn.execute(f"DROP TABLE IF EXISTS stage_ann_{run_id}")
        conn.execute(f"DROP TABLE IF EXISTS stage_batch_{run_id}")


# ---------------------------------------------------------------------------
# 原子发布
# ---------------------------------------------------------------------------
class StalePublishError(Exception):
    """已有更新的版本/运行先发布，本次运行不得覆盖。"""


def _atomic_write_manifest(path: Path, payload: dict) -> None:
    """同目录临时文件 + os.replace 保证清单文件原子替换。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def build_manifest_payload(conn: sqlite3.Connection, run_id: int,
                           source_version: int) -> dict:
    rows = conn.execute(
        "SELECT product_code, batch_no, product_name, produced_at, reason, "
        "source_ann_ids FROM manifest_entries WHERE is_current=1 "
        "ORDER BY product_code, batch_no"
    ).fetchall()
    products: dict[str, list[dict]] = {}
    for r in rows:
        products.setdefault(r["product_code"], []).append(
            {
                "batch_no": r["batch_no"],
                "product_name": r["product_name"],
                "produced_at": r["produced_at"],
                "reason": r["reason"],
                "source_announcement_ids": json.loads(r["source_ann_ids"]),
            }
        )
    return {
        "manifest_version": 1,
        "published_run_id": run_id,
        "source_directory_version": source_version,
        "generated_at": utc_now_iso(),
        "entry_count": len(rows),
        "products": [
            {"product_code": code, "batches": batches}
            for code, batches in sorted(products.items())
        ],
    }


def commit_run(run_id: int, source_version: int) -> dict:
    """把暂存数据整体发布为新清单。失败抛异常，正式清单保持不变。"""
    with _WRITE_LOCK, connect() as conn:
        with write_tx(conn):
            # ---- 旧版本保护：晚完成的旧版本运行不能覆盖先发布的新版本 ----
            meta = conn.execute(
                "SELECT published_run_id, source_version FROM manifest_meta WHERE id=1"
            ).fetchone()
            if meta is not None:
                if source_version < meta["source_version"]:
                    raise StalePublishError(
                        f"源版本 {source_version} 落后于已发布版本 "
                        f"{meta['source_version']}，拒绝覆盖"
                    )
                if (source_version == meta["source_version"]
                        and run_id <= meta["published_run_id"]):
                    raise StalePublishError(
                        f"同版本下运行 {run_id} 早于已发布运行 "
                        f"{meta['published_run_id']}，拒绝覆盖"
                    )

            # ---- 暂存表完整性检查（中断保护：缺表说明运行未完成）----
            staging = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    f"AND name IN ('stage_ann_{run_id}','stage_batch_{run_id}')"
                ).fetchall()
            }
            if f"stage_batch_{run_id}" not in staging:
                raise RuntimeError(f"运行 {run_id} 的暂存数据缺失，拒绝发布")

            # ---- 汇总：同 (产品编码, 批号) 聚合多个来源公告 ----
            staged = conn.execute(
                f"SELECT product_code, batch_no, "
                "MIN(product_name) AS product_name, MIN(produced_at) AS produced_at, "
                "MIN(reason) AS reason, "
                "GROUP_CONCAT(DISTINCT announcement_id) AS ann_ids, COUNT(*) AS row_count "
                f"FROM stage_batch_{run_id} GROUP BY product_code, batch_no"
            ).fetchall()

            # 整体替换正式清单
            conn.execute("UPDATE manifest_entries SET is_current=0")
            conn.execute("DELETE FROM manifest_entries WHERE is_current=0")
            conn.execute("DELETE FROM source_announcements WHERE run_id=?", (run_id,))
            conn.execute(
                "INSERT INTO source_announcements "
                "(run_id, source_version, announcement_id, title, published_at, "
                "withdrawn, withdraw_reason, detail_url, attachment_url) "
                f"SELECT ?, ?, announcement_id, title, published_at, withdrawn, "
                "withdraw_reason, detail_url, attachment_url "
                f"FROM stage_ann_{run_id}",
                (run_id, source_version),
            )

            first_meta = conn.execute(
                "SELECT published_run_id FROM manifest_meta WHERE id=1"
            ).fetchone()
            first_run = first_meta["published_run_id"] if first_meta else run_id

            for r in staged:
                ann_ids = sorted(set(r["ann_ids"].split(",")))
                conn.execute(
                    "INSERT INTO manifest_entries (product_code, batch_no, product_name, "
                    "produced_at, reason, source_ann_ids, source_version, "
                    "first_seen_run, last_updated_run, is_current) "
                    "VALUES (?,?,?,?,?,?,?,?,?,1)",
                    (
                        r["product_code"], r["batch_no"], r["product_name"],
                        r["produced_at"], r["reason"], json.dumps(ann_ids, ensure_ascii=False),
                        source_version, first_run, run_id,
                    ),
                )

            entry_count = len(staged)
            payload = build_manifest_payload(conn, run_id, source_version)

            # 先原子落盘同版 JSON，再提交事务；提交失败则文件随下次发布纠正
            _atomic_write_manifest(config.MANIFEST_PATH, payload)

            conn.execute("DELETE FROM manifest_meta WHERE id=1")
            conn.execute(
                "INSERT INTO manifest_meta (id, published_run_id, source_version, "
                "published_at, entry_count) VALUES (1,?,?,?,?)",
                (run_id, source_version, utc_now_iso(), entry_count),
            )
            conn.execute(
                "UPDATE collection_runs SET status='success', source_version=?, "
                "finished_at=?, batches_collected=? WHERE id=?",
                (source_version, utc_now_iso(), entry_count, run_id),
            )
            conn.execute(f"DROP TABLE IF EXISTS stage_ann_{run_id}")
            conn.execute(f"DROP TABLE IF EXISTS stage_batch_{run_id}")
            return payload


# ---------------------------------------------------------------------------
# 查询（网页 / API）
# ---------------------------------------------------------------------------
def list_manifest(product_code: Optional[str] = None) -> list[dict]:
    sql = ("SELECT * FROM manifest_entries WHERE is_current=1")
    args: tuple = ()
    if product_code:
        sql += " AND product_code=?"
        args = (product_code,)
    sql += " ORDER BY product_code, batch_no"
    with connect() as conn:
        rows = conn.execute(sql, args).fetchall()
        result = []
        for r in rows:
            item = dict(r)
            item["source_ann_ids"] = json.loads(item["source_ann_ids"])
            result.append(item)
        return result


def list_announcements() -> list[dict]:
    meta = get_published_meta()
    if not meta:
        return []
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM source_announcements WHERE run_id=? "
            "ORDER BY announcement_id",
            (meta["published_run_id"],),
        ).fetchall()
        return [dict(r) for r in rows]


def get_announcement(announcement_id: str) -> Optional[dict]:
    meta = get_published_meta()
    if not meta:
        return None
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM source_announcements WHERE run_id=? AND announcement_id=?",
            (meta["published_run_id"], announcement_id),
        ).fetchone()
        return dict(row) if row else None
