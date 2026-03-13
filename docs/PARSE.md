# Parse 阶段详解

> 将 AWS 下载的 zip 文件解析为 parquet，供后续 generate 使用

---

## 1. 输入与输出

| 数据类型 | 输入 | 输出 |
|----------|------|------|
| **Kline** | `aws_data/data/{spot\|futures/um\|futures/cm}/daily/klines/{symbol}/{interval}/*.zip` | `parsed_data/{trade_type}/klines/{symbol}/{interval}/{YYYYMM}.pqt` |
| **Funding** | `aws_data/data/futures/{um\|cm}/monthly/fundingRate/{symbol}/*.zip` | `parsed_data/{trade_type}/funding/{symbol}.pqt` |

**前置条件**：zip 文件必须已通过 verify（存在 `.verified` 标记），否则 parse 会跳过。

---

## 2. Kline Parse 流程

### 2.1 入口

```
parse_all_klines(trade_type, time_interval, force_update)
  → local_list_kline_symbols()  # 从 aws_data 目录扫描已有 symbol
  → parse_klines()              # 多进程解析
```

### 2.2 单 Symbol 解析：`run_parse_symbol_kline`

```
aws_symbol_kline_dir/   (如 BTCUSDT/1m/)
  ├── BTCUSDT-1m-2024-01-01.zip
  ├── BTCUSDT-1m-2024-01-01.zip.CHECKSUM
  ├── BTCUSDT-1m-2024-01-01.zip.verified
  └── ...
```

**步骤**：

1. **收集已校验文件**：`get_verified_aws_data_files()` 只取有 `.verified` 的 zip
2. **按月份分组**：从文件名 `*-YYYY-MM-DD.zip` 提取日期，映射到 partition `YYYYMM`
3. **增量判断**：用 `TSManager.get_row_count_per_date()` 看每个日期已有行数，若 ≥ `thres` 则跳过
   - `thres = 1天 / interval`，如 1m 则 `thres = 1440` 行/天
4. **读取 CSV**：`read_kline_csv()` 解压 zip，读 CSV → Polars
5. **更新分区**：`ts_mgr.update_partition()` 合并新数据到对应 `{YYYYMM}.pqt`

### 2.3 `read_kline_csv` 细节

- **列名**：`candle_begin_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_num`, `taker_buy_*`，丢弃 `close_time`, `ignore`
- **时间**：`candle_begin_time` 为毫秒/微秒时间戳，自动判断并转为 `Datetime(ms)` + UTC
- **首行**：若以 `open_time` 开头则跳过（表头）

### 2.4 增量逻辑

- 已存在 partition 时，`update_partition` 会 **merge**：按 `candle_begin_time` 去重，保留 `keep="last"`
- 只处理「行数不足」的日期，避免重复解析

---

## 3. Funding Parse 流程

### 3.1 入口

```
parse_funding_rates_all(trade_type)
  → local_list_funding_symbols()  # 从 aws_data 目录扫描 symbol
  → parse_funding_rates()         # 多进程解析
```

### 3.2 单 Symbol 解析：`run_parse_symbol_funding`

```
aws_symbol_funding_dir/   (如 BTCUSDT/)
  ├── BTCUSDT-fundingRate-2024-01.zip
  ├── ...
```

**步骤**：

1. 遍历目录下所有 `*.zip`
2. `read_funding_csv()` 逐个解析
3. 合并、按 `candle_begin_time` 排序
4. 写入 **单个** parquet：`parsed_data/{trade_type}/funding/{symbol}.pqt`

### 3.3 `read_funding_csv` 细节

- **列**：`funding_time`, `funding_interval_hours`, `funding_rate`
- **candle_begin_time**：`funding_time` 向下取整到整点（`% (60*60*1000)`），用于与 kline 对齐
- **时间**：转为 `Datetime(ms)` + UTC
- **首行**：若以 `calc_time` 开头则跳过

---

## 4. 存储差异对比

| 项目 | Kline | Funding |
|------|-------|---------|
| 分区 | 按 **月**（TSManager，`YYYYMM.pqt`） | 按 **symbol**（`{symbol}.pqt`） |
| 结构 | `parsed_data/{type}/klines/{symbol}/{interval}/` | `parsed_data/{type}/funding/` |
| 更新 | `update_partition` 增量合并 | 全量覆盖写入 |

---

## 5. 依赖关系

```
aws/checksum.get_verified_aws_data_files  → 只解析已校验的 zip
aws/client_async.AwsKlineClient           → 路径结构 (LOCAL_DIR, get_base_dir)
aws/kline.util.local_list_kline_symbols   → 扫描 aws_data 得到 symbol 列表
aws/funding.util.local_list_funding_symbols
util/ts_manager.TSManager                 → 分区读写、update_partition
util/time.convert_interval_to_timedelta   → 计算 thres (1m→1440)
config.BINANCE_DATA_DIR                   → 根目录
```

---

## 6. 并发与配置

- **进程池**：`ProcessPoolExecutor`，`max_workers=N_JOBS`（默认 `cpu_count-2`）
- **spawn**：`mp.get_context("spawn")` 避免 fork 问题
- **mp_env_init**：限制 Polars/NumPy 线程，避免 oversubscribe

---

## 7. 关键代码位置

| 功能 | 文件 |
|------|------|
| Kline 解析入口 | `aws/kline/parse.py` → `parse_all_klines` |
| Kline 单文件读取 | `aws/kline/parse.py` → `read_kline_csv` |
| Kline 单 symbol 逻辑 | `aws/kline/parse.py` → `run_parse_symbol_kline` |
| Funding 解析入口 | `aws/funding/parse.py` → `parse_funding_rates_all` |
| Funding 单文件读取 | `aws/funding/parse.py` → `read_funding_csv` |
| 已校验文件筛选 | `aws/checksum.py` → `get_verified_aws_data_files` |
| 分区管理 | `util/ts_manager.py` → `TSManager` |
