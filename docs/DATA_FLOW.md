# binance_datatool 数据流概览

> 简明数据流文档，便于选择性深入

---

## 1. 入口与结构

| 入口 | 用途 |
|------|------|
| `datalake.py` | 主 CLI（Typer），4 个子命令 |
| `main.sh` | 全流程编排脚本 |
| `python -m api.okx` | OKX 工具独立入口 |

### 目录职责

```
api/       → Binance / Bybit / OKX REST API 客户端
aws/       → Binance AWS S3 数据（kline、funding、liquidation）
config/    → 路径、枚举、并发配置
generate/  → K 线合并、重采样、VWAP、funding 关联
util/      → 日志、时间、分区存储、symbol 过滤
```

---

## 2. 数据源

| 来源 | 数据类型 | 格式 |
|------|----------|------|
| **Binance AWS S3** | K 线（日 zip）、Funding（月 zip） | CSV in zip |
| **Binance REST API** | K 线、Funding | JSON |
| **Bybit API** | K 线、Funding | JSON |
| **OKX API** | K 线、Funding | JSON |

---

## 3. 主数据流（Binance）

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  DOWNLOAD   │ ──► │   VERIFY    │ ──► │    PARSE    │ ──► │  API FILL   │ ──► │   GENERATE  │ ──► │  RESAMPLE   │
│  (aria2c)   │     │  (SHA256)   │     │ (zip→pqt)   │     │ (可选补全)   │     │ (merge+VWAP) │     │  (1m→1h)    │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      │                    │                    │                    │                    │                    │
      ▼                    ▼                    ▼                    ▼                    ▼                    ▼
  aws_data/           .verified            parsed_data/           api_data/          results_data/       results_data/
  (zip+CHECKSUM)      (校验通过)           (parquet)              (parquet)          1m/                 1h/
```

### 阶段说明

| 阶段 | 命令 | 输出 |
|------|------|------|
| **Download** | `aws_kline download-*` / `aws_funding download-*` | `aws_data/` 下 zip + CHECKSUM |
| **Verify** | `aws_* verify-type-all` | SHA256 校验，生成 `.verified` |
| **Parse** | `aws_* parse-type-all` | zip → Polars → parquet，按 TSManager 分区 |
| **API Fill** | `api_data download-recent-funding-type` 等 | 补全近期 funding、缺失 kline |
| **Generate** | `generate kline-type` | 合并 parsed + api，加 VWAP、funding，gap 处理 |
| **Resample** | `generate resample-type` | 1m → 1h 聚合 |

---

## 4. 存储布局

```
~/dev/babylake/
├── binance_data/
│   ├── aws_data/           # 原始下载
│   │   └── data/{spot|futures/um|futures/cm}/{daily|monthly}/{klines|fundingRate}/
│   ├── parsed_data/        # 解析后
│   │   └── {spot|um_futures|cm_futures}/{klines|funding}/
│   ├── api_data/           # API 补全
│   │   └── {trade_type}/{klines|funding_rate}/
│   └── results_data/       # 最终结果
│       └── {trade_type}/{1m|1h}/
├── bybit_data/
└── okx_data/
```

- **K 线**：按 `TSManager` 月分区，`{YYYYMM}.pqt`
- **Funding**：按 symbol 存 parquet

---

## 5. main.sh 执行顺序（Spot + UM）

```
Spot:
  aws_kline download-spot → verify → parse
  → generate kline (--split-gaps --with-vwap --no-with-funding-rates)
  → resample 1h

UM Futures:
  aws_funding download → verify → parse
  → api_data download-recent-funding
  aws_kline download → verify → parse
  → generate kline (--with-funding-rates)
  → resample 1h
```

---

## 6. 核心模块关系

```
datalake.py
├── aws_funding  → download, verify, parse
├── aws_kline    → download, verify, parse
├── api_data     → Binance/Bybit API 补全
└── generate     → kline 合并、resample

依赖:
  config        → BINANCE_DATA_DIR, TradeType, N_JOBS
  util.ts_manager → 分区读写 (YYYYMM.pqt)
  util.symbol_filter → 按 quote/contract 过滤
  aws.client_async → S3 列表、aria2c 下载
  aws.checksum   → SHA256 校验
```

---

## 7. 交易类型与交易所

| TradeType | 说明 |
|-----------|------|
| spot | 现货（USDT/USDC/BTC 等） |
| um_futures | USDⓈ-M 永续/交割 |
| cm_futures | Coin-M 永续/交割 |

| Exchange | 支持 |
|----------|------|
| binance | AWS + API 全流程 |
| bybit | API 为主（main.sh 中注释） |
| okx | API 为主（main.sh 中注释） |

---

## 8. 可深入模块

| 模块 | 关注点 |
|------|--------|
| `aws/client_async.py` | S3 列表、aria2c 批量下载 |
| `aws/checksum.py` | 校验逻辑 |
| `util/ts_manager.py` | 分区策略、读写 |
| `generate/kline.py` | merge、VWAP、gap 检测与 split |
| `generate/resample.py` | 1m→1h 聚合规则 |
| `aws/liquidation/` | 已实现但未接入主流程 |
