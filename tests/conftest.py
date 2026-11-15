"""端到端测试夹具：在随机端口启动真实的演示源与采集端 uvicorn 服务。"""
from __future__ import annotations

import os
import socket
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import uvicorn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ServerThread:
    def __init__(self, app_path: str, port: int):
        self.port = port
        config = uvicorn.Config(app_path, host="127.0.0.1", port=port,
                                log_level="warning", lifespan="on")
        self.server = uvicorn.Server(config)
        import threading
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


def _wait_ready(base_url: str, timeout: float = 15) -> None:
    deadline = time.time() + timeout
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{base_url}/health", timeout=2)
            if resp.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        time.sleep(0.2)
    raise RuntimeError(f"服务未就绪：{base_url}（{last_exc}）")


@pytest.fixture(scope="session")
def env_dirs(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("data")
    os.environ["COLLECTOR_DB"] = str(data_dir / "collector.db")
    os.environ["MANIFEST_PATH"] = str(data_dir / "stop_sale_manifest.json")
    return data_dir


@pytest.fixture(scope="session")
def servers(env_dirs):
    feed_port, app_port = _free_port(), _free_port()
    os.environ["FEED_BASE_URL"] = f"http://127.0.0.1:{feed_port}"
    os.environ["HTTP_TIMEOUT"] = "8"

    # 延迟导入，确保环境变量先就位
    from app import config as app_config  # noqa: F401
    import app.db as db
    db.config.DB_PATH = Path(os.environ["COLLECTOR_DB"])
    db.config.MANIFEST_PATH = Path(os.environ["MANIFEST_PATH"])
    import app.main as main_mod
    main_mod.config.FEED_BASE_URL = os.environ["FEED_BASE_URL"]
    import app.collector as collector_mod
    collector_mod.config.FEED_BASE_URL = os.environ["FEED_BASE_URL"]

    feed = ServerThread("demo_feed.server:app", feed_port)
    app = ServerThread("app.main:app", app_port)
    feed.start(); app.start()
    _wait_ready(f"http://127.0.0.1:{feed_port}")
    _wait_ready(f"http://127.0.0.1:{app_port}")
    try:
        yield f"http://127.0.0.1:{feed_port}", f"http://127.0.0.1:{app_port}"
    finally:
        app.stop(); feed.stop()


@pytest.fixture(autouse=True)
def fresh_db(env_dirs):
    """每个用例使用独立的 SQLite 与清单文件，避免运行历史互相影响。"""
    import app.db as db
    db_path = env_dirs / f"test_{uuid4().hex}.db"
    manifest_path = env_dirs / f"manifest_{uuid4().hex}.json"
    db.config.DB_PATH = db_path
    db.config.MANIFEST_PATH = manifest_path
    db.init_db()
    yield
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(db_path) + suffix)
        p.unlink(missing_ok=True)
    manifest_path.unlink(missing_ok=True)


@pytest.fixture(autouse=True)
def reset_feed(servers):
    """每个用例前后把演示源恢复到 v1 初始状态。"""
    feed_url, _ = servers
    httpx.post(f"{feed_url}/control/reset", timeout=5)
    yield
    httpx.post(f"{feed_url}/control/reset", timeout=5)


@pytest.fixture
def urls(servers):
    return servers


def start_run(app_url: str) -> int:
    resp = httpx.post(f"{app_url}/api/collect", timeout=10)
    assert resp.status_code == 200, resp.text
    return resp.json()["run_id"]


def wait_run(app_url: str, run_id: int, timeout: float = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        runs = httpx.get(f"{app_url}/api/runs", timeout=10).json()
        run = next(r for r in runs if r["id"] == run_id)
        if run["status"] != "running":
            return run
        time.sleep(0.2)
    raise AssertionError(f"运行 {run_id} 超时未结束")


def manifest(app_url: str) -> dict:
    return httpx.get(f"{app_url}/api/manifest", timeout=10).json()


def batch_keys(app_url: str) -> set[tuple[str, str]]:
    return {(e["product_code"], e["batch_no"]) for e in manifest(app_url)["entries"]}


def stop_sale_json(app_url: str) -> dict:
    return httpx.get(f"{app_url}/stop-sale/manifest.json", timeout=10).json()


def wait_feed_requests(feed_url: str, key: str, minimum: int = 1,
                       timeout: float = 15) -> None:
    """轮询演示源请求计数，直到指定端点被访问 minimum 次。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = httpx.get(f"{feed_url}/control/state", timeout=5).json()
        if state.get("request_counts", {}).get(key, 0) >= minimum:
            return
        time.sleep(0.15)
    raise AssertionError(f"等待演示源 {key} 请求数 >= {minimum} 超时")
