# AWS Parse → ArcticDB

> 将 `aws_clone` 下载并校验过的 zip 解析进 ArcticDB。  
> 实现位于 `src/aws_parse/`，**不修改**旧版 `aws/*/parse.py`。

---

## 1. 两步流程

```
verified zips (aws_data/)
        │
        ├─► parse-klines  ──► Arctic klines_{interval}
        │
        └─► parse-funding ──► Arctic funding
                    │
                    ▼
              merge-wide  ──► Arctic klines_{interval}_wide
```

| 步骤 | 命令 | Arctic symbol | 增量单元 |
|------|------|---------------|----------|
| 1a | `parse-klines` | `klines_1m` | 按**日**横截面 `update` |
| 1b | `parse-funding` | `funding` | 按**月**横截面 `update` |
| 2 | `merge-wide` | `klines_1m_wide` | 按**日** left-join 后 `update` |

三张表 MultiIndex 均为 **`(candle_begin_time, symbol)`**（时间在外层）。

---

## 2. 入口

统一入口 `main.py`：

```bash
uv run python main.py parse klines --trade-type um_futures --interval 1m
uv run python main.py parse funding --trade-type um_futures
uv run python main.py parse merge-wide --trade-type um_futures --interval 1m
```

常用选项：

| 参数 | 说明 |
|------|------|
| `--force` | 重写已存在的日/月 |
| `--symbols BTCUSDT,ETHUSDT` | 仅处理这些 symbol（会与当日已有横截面合并后再 `update`，避免误删其它 symbol） |
| `--aws-data-dir` | 默认 `~/dev/babylake/binance_data/aws_data` |
| `--arctic-uri` | 默认 `lmdb://~/dev/babylake/binance_data/arctic` |

前置：对应 zip 必须有 `.verified` 标记（与 clone 一致）。

---

## 3. 存储布局

| 项 | 值 |
|----|-----|
| URI | `lmdb://{BINANCE_DATA_DIR}/arctic` |
| Library | `{trade_type}`（`um_futures` / `cm_futures` / `spot`） |

| Symbol | 列 |
|--------|-----|
| `klines_{interval}` | OHLCV、`quote_volume`、`trade_num`、`taker_buy_*` |
| `funding` | `funding_rate`、`funding_interval_hours` |
| `klines_{interval}_wide` | kline 全部列 + `funding_rate`（缺省填 `0`） |

Spot 无 funding：`parse-funding` 直接跳过；`merge-wide` 写 `funding_rate=0`。

---

## 4. `update` 约定

ArcticDB `update` 按**最外层时间**整段替换。同日只提交部分 symbol 会清掉该日其它 symbol。

本模块因此：

- 全市场增量：一日/一月内提交**该批全部 symbol**
- `--symbols`：先读出该区间已有行，保留未选中的 symbol，再合并写入

查询请用 `date_range` / `columns`，不要整表 `read()`。

---

## 5. 模块结构

```
src/aws_parse/
├── app.py              # Typer（挂到 main.py parse）
├── paths.py            # aws_data / Arctic 路径
├── store.py            # MultiIndex + update/read
├── parse_klines.py     # Step1a：CSV 读取 + 按日 ingest
├── parse_funding.py    # Step1b：CSV 读取 + 按月 ingest
└── merge_wide.py       # Step2
main.py                 # 仓库根统一入口
```

---

## 6. 与旧管线 / clone 的关系

| | 旧 `aws/*/parse` | 新 `src/aws_parse` |
|--|------------------|---------------------|
| 输出 | parquet / TSManager | ArcticDB LMDB |
| 改动旧代码 | — | **不改** |
| 输入 | 同 `aws_data` + `.verified` | 同左（与 `src/aws_clone` 衔接） |

下游 `generate` 仍读 parquet，直到后续单独改为读 Arctic。
