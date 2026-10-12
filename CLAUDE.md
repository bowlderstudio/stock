# CLAUDE.md

本文件为 Claude Code 在本仓库工作时提供指引。

## 项目概述

挪威股票（Oslo Børs / Euronext Expand Oslo / Euronext Growth Oslo）证券清单与 OHLCV 日线历史数据采集系统，个人研究用途，目标运行成本 $0/月。

- 完整设计：`docs/norway_stock_data_system_design.md`（修改架构、表结构或 Flow 前先读它，改完同步更新文档）
- 当前状态：`src/stockdata/` 骨架已建立。`config.py`、`trading_calendar.py`、`models.py` 已实现；其余函数签名和 docstring 已定，函数体为 `NotImplementedError`，待按 TDD 逐个实现。`yfin.py` 是早期试验脚本；`scripts/yahoo_ip_test.py` 用于检测 Yahoo 对当前网络（含云端 IP）是否可用。

技术栈：Python ≥3.11、uv、yfinance、Prefect Cloud Hobby（Serverless 执行）、Neon PostgreSQL（设计文档仍写 Supabase，待更新）。连接串来自 `.env` 的 `STOCKDATA_DB_URL` 或 `DATABASE_URL`（asyncpg 写法会自动转换）。

## 常用命令

```bash
uv sync                      # 安装依赖
uv add <pkg>                 # 添加依赖（不要手改 uv.lock）
uv run pytest                # 运行测试
uv run pytest tests/test_quality.py::test_name   # 单个测试
uv run ruff check src tests scripts && uv run ruff format src tests   # lint + 格式化
uv run python scripts/yahoo_ip_test.py   # Yahoo 可用性测试（访问网络）
uv run python scripts/apply_sql.py sql/001_init.sql   # 对 .env 中的数据库执行 SQL
uv run python scripts/load_yahoo_history.py           # 临时回填脚本（Flow A/C 完成前使用）

# 数据库测试（tests/test_db.py）需要本地一次性 PostgreSQL，不可用时自动 skip：
docker run -d --name stockdata-test-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=stockdata_test \
  -p 127.0.0.1:55432:5432 postgres:17
```

## 代码结构（见设计文档 §11）

```
sql/001_init.sql             # 表结构，按编号追加迁移，不改已应用的文件
src/stockdata/
  config.py                  # pydantic-settings，环境变量前缀 STOCKDATA_，所有可调参数在这里
  db.py                      # psycopg 连接（prepare_threshold=None）与 upsert
  models.py                  # dataclass 与状态/错误类型枚举，与 SQL 保持一致
  quality.py                 # 纯函数质量检查：OHLC、拆股跳变、数据缺口
  flows.py                   # Prefect @flow 入口，保持薄壳，逻辑放 pipelines/
  trading_calendar.py        # XOSL 交易日历（不要命名为 calendar.py，会遮蔽标准库）
  sources/euronext.py        # 证券清单适配器（含手动 CSV 回退）
  sources/yahoo.py           # yfinance 封装：批量、限流、重试
  pipelines/instruments.py   # Flow A: sync_instruments
  pipelines/prices.py        # Flow B/C/D: 日增量、回填、近期校验
  pipelines/corporate_actions.py  # 拆股/分红检测，触发全量重取
.github/workflows/           # yahoo-ip-test.yml（手动）；backup.yml（计划中，每周 pg_dump）
scripts/import_euronext_csv.py
tests/
```

四个 Prefect Flow（Hobby 上限 5 个 deployment，新增前先确认额度）：
- `sync_instruments` 每周；`sync_daily_prices` 交易日夜间（Europe/Oslo）；`backfill_history` 手动；`reconcile_recent_prices` 每周。

## 必须遵守的约束

数据正确性
- yfinance 调用必须显式传 `auto_adjust=False`（yfinance 1.x 默认是 True），同时保存 `Close` 和 `Adj Close`，绝不混用。
- 注意：Yahoo 的 "Close" 已做拆股调整，并非交易所原始成交价；发生拆股时整段历史都会变化，短期回看无法修正。因此拆股/分红写入 `corporate_actions`，发现新事件即设置 `instrument_sync_state.needs_full_refetch`，由 `backfill_history` 全量重取该股票。
- 报价币种以 Euronext 清单为准，不能默认 NOK（Oslo 有 USD/EUR 报价的证券）。
- 回填进度、最近价格日期、连续失败次数存在 `instrument_sync_state`，回填必须可断点续跑。
- 价格表外键用内部 `instrument_id`，不要用 ticker 或 ISIN 当主键（两者都会变）。
- 一次抓取失败或空结果不等于退市：只能标 `inactive_candidate`，由人工确认。永远不删除已入库的价格历史（退市后 Yahoo 往往不再提供数据）。
- 缺失交易日要按 Oslo 交易日历判断（`exchange_calendars` 的 `XOSL`），不能把周一至周五都当交易日。
- 不要写入当天未收盘的盘中数据。

幂等与容错
- 所有写入使用 `INSERT ... ON CONFLICT DO UPDATE`，重复运行不得产生重复行。
- 单个 ticker 失败不得阻断整批；成功行先提交，失败写入 `ingestion_errors`。
- 429/5xx 用指数退避 + jitter，有重试上限；404/无效代码标记 `mapping_review`，不重试。
- 批量、并发、延迟等参数放配置，不要硬编码（设计值：每批 20–30 个 symbol、并发 2、批间 2–5 秒）。

免费额度
- Prefect Serverless 500 分钟/月：每个 Flow 要限制单次最长执行时间，回填必须可断点续跑。
- Supabase 500 MB：改表结构时考虑行宽和索引体积。

安全
- 凭证（`STOCKDATA_DB_URL`、`PREFECT_API_KEY`）只来自环境变量 / Prefect Secret，提供 `.env.example`，`.env` 不入库。
- 日志不得输出连接串或凭证。
- Supabase 直连地址默认仅 IPv6；从 Serverless/CI 连接通常需要用 Supavisor pooler 连接串。使用 transaction 模式（6543 端口）时要禁用 prepared statements。

## 测试

- `quality.py`、ticker 映射等纯逻辑必须有单元测试，不访问网络。
- 不要在测试里调用真实 Yahoo/Euronext；用录制的样本数据或 mock。
- upsert 测试需要真实 PostgreSQL（本地容器，见上），绝不连 Neon 生产库。

## Git

- 本目录是独立仓库（branch main，remote bowlderstudio/stocks）。注意上级目录 `/home/minshi/workdata/git` 另有一个 .git，提交前用 `git rev-parse --show-toplevel` 确认在本仓库。
- 不要主动 `git add` / `git commit` / `git push`，除非用户明确要求。
