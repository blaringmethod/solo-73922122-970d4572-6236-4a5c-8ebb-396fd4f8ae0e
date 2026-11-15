"""运行配置：全部可通过环境变量覆盖，便于容器与测试使用。"""
from __future__ import annotations

import os
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


# 演示源基地址
FEED_BASE_URL = _env("FEED_BASE_URL", "http://feed:8010")
# SQLite 数据库文件
DB_PATH = Path(_env("COLLECTOR_DB", "/data/collector.db"))
# 停售系统读取的同版 JSON 清单文件（原子替换）
MANIFEST_PATH = Path(_env("MANIFEST_PATH", "/data/stop_sale_manifest.json"))
# HTTP 超时（连接、读取），秒
HTTP_TIMEOUT = float(_env("HTTP_TIMEOUT", "15"))
# 同一时刻只允许一个采集任务
SINGLE_RUN_GUARD = _env("SINGLE_RUN_GUARD", "1") not in ("0", "false", "False")
