# 食品安全召回采集与停售清单系统

从召回公示站采集公告与批号附件，为停售系统维护**应拦截的产品批次清单**。

- **采集端**（FastAPI + SQLite，端口 `8000`）：发起采集、查看按「产品编码 + 批号」汇总的当前清单与来源公告，并向停售系统提供同版 JSON。
- **演示源**（FastAPI，端口 `8010`）：本地 HTTP 召回公示站，按递增目录版本 `v1 → v2 → v3` 分页提供公告与 UTF-8 CSV 附件，支持切换版本、撤回公告、注入附件故障与响应延时。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 采集端操作面板 | http://localhost:8000/ | 发起/取消采集、查看运行历史与失败原因 |
| 当前拦截清单 | http://localhost:8000/manifest | 按产品编码/批号汇总，含来源公告与撤回状态 |
| 停售系统 JSON | http://localhost:8000/stop-sale/manifest.json | 与落盘文件 `/data/stop_sale_manifest.json` 同版 |
| 演示源控制台 | http://localhost:8010/ | 切版本、撤回公告、附件故障、延时注入 |

无 Docker 时可本地启动（需要 Python 3.11，`pip install -r requirements.txt`）：

```bash
./scripts/run_local.sh
```

## 浏览器演示剧本

1. 打开 http://localhost:8000/ ，点击 **发起采集**：读全 v1 两页公告与 3 个有效附件，发布 7 个拦截批号。
2. 打开 http://localhost:8010/ ，切换到 **v3** 后再次采集：
   - R001、R003 已撤回，其独有批号解除拦截；
   - 批号 `SP-YS-210 / B20261026S` 虽随 R003 撤回，但仍被**有效公告 R005** 覆盖，拦截不解除（清单中来源显示为 R005）。
3. 在演示源把某附件故障设为 **HTTP 500** 或 **损坏字节**，再次采集：运行失败并显示原因，**上一轮清单原样保留**。
4. 演示源设若干秒延时后发起采集，在采集端点击 **取消本次采集**：运行记为已取消，清单不变。
5. 发起采集后立刻在演示源切换版本：翻页响应的 `X-Directory-Version` 与钉住版本不一致，采集中止并保留上轮清单。
6. 同版本下在演示源「撤回」一则公告后重新采集：清单即时更新；停售 JSON 与页面数据始终同版。

## 核心正确性保证

### 1. 一次采集读全同一目录版本

首个请求即从响应头 `X-Directory-Version` **钉住版本**，之后每个分页/附件响应都校验该头；版本不一致立即以 `VERSION_CHANGED` 失败。分页链（`next_page`、`total_pages`、页码连续性、末页 `next_page=null`）与公告号唯一性均做完整性校验。已撤回公告不下载附件、不产生拦截批号。

### 2. 成功后整体替换，失败保留上轮清单

抓取结果先写入以 `run_id` 命名的 SQLite 暂存表（`stage_ann_*`、`stage_batch_*`）；全部成功后在**单个事务**内清空并重建正式清单，同时用「同目录临时文件 + `os.replace` + `fsync`」原子替换 JSON 文件。任何失败（网络、HTTP 错误、CSV 非 UTF-8/缺列、分页异常、取消）都只清理暂存数据，正式表与文件保持上轮内容，并在运行历史中记录原因码与详情。

### 3. 撤回不解除仍被覆盖的批次

正式清单以 `(product_code, batch_no)` 为唯一键（部分唯一索引 `ux_manifest_current`），来源公告号聚合存储。撤回一则公告后重新采集，仅当没有其他有效公告再覆盖该批号时才会从清单消失。

### 4. 并发采集的旧版本保护

提交事务内校验：源版本小于已发布版本，或同版本下 `run_id` 不大于已发布运行，一律抛 `StalePublishError`，运行标记为 `stale`。因此并发采集里**晚完成的旧版本无法覆盖先发布的新版本**。API 默认同时只允许一个运行（第二个请求返回 409），底层守卫独立于该限制生效。

## 原因码

| 原因码 | 含义 |
| --- | --- |
| `FEED_UNREACHABLE` | 无法连接公示源 / 请求超时 |
| `FEED_HTTP_ERROR` | 公告分页返回非 200 |
| `VERSION_CHANGED` | 采集过程中目录版本变化 |
| `PAGE_INCOMPLETE` | 分页链断裂或分页字段不一致 |
| `ATTACHMENT_HTTP_ERROR` | 附件下载失败（如模拟 500） |
| `ATTACHMENT_CORRUPT` | 附件非合法 UTF-8 或内容损坏 |
| `ATTACHMENT_BAD_FORMAT` | 附件缺必需列或批号行为空 |
| `CANCELLED` | 手动取消（请求中断） |
| `STALE_VERSION` | 旧版本/旧运行提交被拒绝 |
| `UNEXPECTED` | 未预期错误（含详情） |

## 目录结构

```
app/                    采集端
  config.py             环境变量配置
  db.py                 SQLite  schema、暂存、原子发布、查询
  collector.py          版本钉住、分页/附件抓取、CSV 严格解析、运行编排
  main.py               FastAPI 路由与采集任务管理
  templates/ static/    操作面板与清单页面
demo_feed/              演示源
  server.py             分页/附件接口 + /control/* 故障注入
  data/v{1,2,3}/        递增目录版本：pages/page_*.json + attachments/*.csv
tests/                  16 个端到端/单元测试
scripts/run_local.sh    无容器本地启动
docker-compose.yml      feed(8010) + collector(8000)
```

## 数据库与文件

- SQLite（WAL 模式）表：`collection_runs`（运行历史与原因）、`source_announcements`（发布版本的公告快照，含撤回状态）、`manifest_entries`（当前拦截批号，部分唯一索引保证键唯一）、`manifest_meta`（单行发布指针）。
- 停售 JSON：`/data/stop_sale_manifest.json`，按产品分组，每个批号带 `source_announcement_ids`；HTTP 入口 `/stop-sale/manifest.json` 返回同一文件内容。

## 测试

```bash
python -m pytest tests/ -q
```

覆盖：全量采集与同版 JSON、静态/运行时撤回、撤回覆盖保留、附件 500 与损坏、翻页中途切版本、手动取消、并发旧版本拒绝、409 单运行保护、页面渲染、UTF-8 中文 CSV，以及发布守卫与来源聚合的数据库单元测试。
