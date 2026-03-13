# Generate Resample 详解

> 将 1m kline 聚合为更高周期（如 1h、4h），供下游使用

---

## 1. 输入与输出

| 输入 | 输出 |
|------|------|
| `results_data/{type}/1m/{symbol}.pqt` | `results_data/{type}/{resample_interval}/{symbol}.pqt` |

**前置**：必须先跑完 `generate kline-type ... 1m`，否则无 1m 数据可重采样。

---

## 2. 主流程

```
resample_kline_all(exchange, trade_type, resample_interval)
  → 获取 1m 目录下所有 symbol
  → 删除已有 resample 目录
  → 多进程：每个 symbol 调用 resample_kline()
```

### 单 Symbol 流程：`resample_kline`

```
1. 读取 results_data/{type}/1m/{symbol}.pqt
2. [仅 um_futures] 加 spot_exist：检查 spot 是否有该 symbol 数据，标记时间范围
3. polars_calc_resample：group_by_dynamic 聚合
4. 写入 results_data/{type}/{resample_interval}/{symbol}.pqt
```

---

## 3. 聚合逻辑：`polars_calc_resample`

使用 `group_by_dynamic("candle_begin_time", every=resample_interval)` 按时间窗口聚合。

### 3.1 通用列

| 列 | 聚合规则 |
|----|----------|
| symbol | last |
| open | first |
| high | max |
| low | min |
| close | last |
| volume | sum |
| quote_volume | sum |

### 3.2 Binance 额外列

| 列 | 聚合规则 |
|----|----------|
| trade_num | sum |
| taker_buy_base_asset_volume | sum |
| taker_buy_quote_asset_volume | sum |

### 3.3 可选列

| 列 | 聚合规则 |
|----|----------|
| spot_exist | first（futures 专用） |
| avg_price_1m | first |
| funding_rate | 首个 \|rate\| > 1e-6 的值 |
| funding_price | 对应 open |
| funding_time | 对应 candle_begin_time |

funding 若窗口内无有效值，填 0。

### 3.4 `resample_interval` 格式

Polars `every` 支持：`"1h"`, `"4h"`, `"30m"` 等，与 `convert_interval_to_timedelta` 兼容。

---

## 4. spot_exist 逻辑（仅 um_futures）

- 目的：标记该 futures 在对应时间段是否有 spot 交易对
- 查找：`results_data/spot/1m/` 下 `{symbol}.pqt` 或 `1000{symbol}` 等变体
- `ONLY_SWAP`：部分 symbol 仅 swap 无 spot，若找到 spot 会 warning
- 结果：`spot_exist` = 该 1m 时间是否落在 spot 的 [min, max] 内

---

## 5. CLI 用法

```bash
python datalake.py generate resample-type binance spot 1h
python datalake.py generate resample-type binance um_futures 1h
```

---

## 6. 依赖关系

```
config.BINANCE_DATA_DIR, BYBIT_DATA_DIR, OKX_DATA_DIR
util.concurrent.mp_env_init
```

---

## 7. 关键代码位置

| 功能 | 文件 |
|------|------|
| 入口 | `generate/app.py` → `resample_type` |
| 主逻辑 | `generate/resample.py` → `resample_kline_all` |
| 单 symbol | `generate/resample.py` → `resample_kline` |
| 聚合计算 | `generate/resample.py` → `polars_calc_resample` |
