# Generate Kline 详解

> 合并 parsed + api 数据，加 VWAP/funding，做 gap 检测与填充，输出最终 1m kline

---

## 1. 输入与输出

| 交易所 | 输入 | 输出 |
|--------|------|------|
| **Binance** | `parsed_data/{type}/klines/{symbol}/{interval}/` + `api_data/{type}/klines/` | `results_data/{type}/{interval}/{symbol}.pqt` |
| **Binance** (funding) | `parsed_data/{type}/funding/` + `api_data/{type}/funding_rate/` | 同上（join 到 kline） |
| **Bybit** | `bybit_data/linear/klines/{symbol}/{interval}/` | `bybit_data/results_data/linear/{interval}/` |
| **OKX** | `okx_data/swap/klines/{interval}/{symbol}.pqt` | `okx_data/results_data/swap/{interval}/` |

---

## 2. 主流程：`gen_kline` → `gen_kline_type`

```
gen_kline_type(exchange, trade_type, time_interval, ...)
  → 获取 symbol 列表（按交易所不同来源）
  → 清空 results_dir
  → 多进程：每个 symbol 调用 gen_kline()
```

### 单 Symbol 流程：`gen_kline`

```
1. 读取 kline 数据（按 exchange 选择路径）
2. [可选] 计算 VWAP：quote_volume / volume → avg_price_{interval}
3. [可选] 合并 funding：join funding_rate，无则填 0
4. [可选] split_gaps：检测 gap → 按 gap 切分 → 生成 SP0_xxx, SP1_xxx, ... 或保留原名
5. fill_kline_gaps：补全缺失时间戳
6. 加 symbol 列，写入 parquet
```

---

## 3. 核心函数

### 3.1 `merge_klines`（Binance）

- 读 `parsed_data`（TSManager.read_all）
- 读 `api_data` 下 `*.pqt`
- `concat` → `unique(candle_begin_time, keep="last")` → `sort`
- `exclude_empty=True` 时过滤 `volume > 0`

### 3.2 `merge_funding_rates`（Binance）

- 读 `parsed_data/.../funding/{symbol}.pqt`
- 读 `api_data/.../funding_rate/{symbol}.pqt`
- 同上 merge 逻辑，只保留 `candle_begin_time`, `funding_rate`

### 3.3 `scan_gaps`

检测满足条件的 gap：

- `time_diff > min_days`（相邻 candle 时间差）
- `|price_change| > min_price_chg`（open/prev_close - 1）

或第二类：`time_diff > min_days*2`，不要求价格变化。

返回：`prev_begin_time`, `candle_begin_time`, `prev_close`, `open`, `time_diff`, `price_change`

### 3.4 `split_by_gaps`

- 无 gap：返回 `{symbol: df}`
- 有 gap：按 gap 切分，前几段命名为 `SP0_{symbol}`, `SP1_{symbol}`，最后一段保留 `symbol`

### 3.5 `fill_kline_gaps`

- 生成 `candle_begin_time` 的完整时间序列（按 interval）
- left join 原数据
- 缺失行：`close` forward fill，`open/high/low` 用 `close`，`volume/quote_volume` 填 0
- 有 `avg_price_1m`：forward fill，clip 到 [low, high]
- 有 `funding_rate`：填 0
- Binance 额外：`trade_num`, `taker_buy_*` 填 0

---

## 4. CLI 参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `split_gaps` | False | 是否按 gap 切分 |
| `min_days` | 1 | gap 最小天数 |
| `min_price_chg` | 0.1 | gap 最小价格变化比例 |
| `with_vwap` | True | 是否计算 VWAP |
| `with_funding_rates` | False | 是否合并 funding（仅 futures） |

---

## 5. 依赖关系

```
aws.kline.util.local_list_kline_symbols  → Binance symbol 列表
config.BINANCE_DATA_DIR, BYBIT_DATA_DIR, OKX_DATA_DIR
util.ts_manager.TSManager
util.time.convert_interval_to_timedelta
```

---

## 6. 关键代码位置

| 功能 | 文件 |
|------|------|
| 入口 | `generate/app.py` → `kline_type` |
| 主逻辑 | `generate/kline.py` → `gen_kline_type`, `gen_kline` |
| 合并 kline | `generate/kline.py` → `merge_klines` |
| 合并 funding | `generate/kline.py` → `merge_funding_rates` |
| Gap 检测 | `generate/kline.py` → `scan_gaps` |
| Gap 切分 | `generate/kline.py` → `split_by_gaps` |
| 补全缺失 | `generate/kline.py` → `fill_kline_gaps` |
