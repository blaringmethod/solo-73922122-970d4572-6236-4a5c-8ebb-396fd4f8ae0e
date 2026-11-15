#!/usr/bin/env bash
# 本地无容器时的双服务启动脚本（演示源 :8010，采集端 :8000）
set -euo pipefail
cd "$(dirname "$0")/.."

export FEED_BASE_URL="${FEED_BASE_URL:-http://127.0.0.1:8010}"
export COLLECTOR_DB="${COLLECTOR_DB:-$(pwd)/data/collector.db}"
export MANIFEST_PATH="${MANIFEST_PATH:-$(pwd)/data/stop_sale_manifest.json}"
mkdir -p "$(dirname "$COLLECTOR_DB")"

python -m uvicorn demo_feed.server:app --host 127.0.0.1 --port 8010 &
FEED_PID=$!
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 &
APP_PID=$!
trap 'kill $FEED_PID $APP_PID 2>/dev/null || true' INT TERM EXIT

echo "演示源控制台:  http://127.0.0.1:8010/"
echo "采集端面板:    http://127.0.0.1:8000/"
wait
