# AWS Prefix Clone

> 按 Binance Vision S3 的 `prefix` 把远端对象镜像到本地，并做 SHA256 校验。  
> 实现位于 `src/aws_clone/`，不依赖旧版 `aws/` 下载逻辑。

---

## 1. 做什么

给定一个 S3 prefix（可含路径段级通配符 `*`），模块会：

1. **展开** `*` → 具体前缀列表  
2. **异步列出** 前缀下全部对象 key  
3. **aria2c 下载** 到本地，路径与 S3 key 一一对应  
4. **SHA256 校验** zip，通过则写 `.verified`；失败则删除坏文件便于重下  

等价于：按你给的 prefix **clone** 一段 `data.binance.vision` 到本地。

| 项 | 值 |
|----|-----|
| 远端 Base | `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision` |
| 默认本地根目录 | `~/dev/babylake/binance_data/aws_data` |
| 本地布局 | `{DEFAULT_AWS_DATA_DIR}/{s3_key}` |

---

## 2. 入口

与仓库里 `datalake.py` / `aws/*/app.py` 一样，用 Typer。

```bash
uv run python main.py clone "data/futures/um/daily/klines/*/1m/"
uv run python main.py clone "data/futures/um/daily/klines/BTCUSDT/1m/"
```

| 参数 | 说明 |
|------|------|
| `PREFIX` | 必填。S3 prefix，可含路径段 `*` |

本地始终写到 `~/dev/babylake/binance_data/aws_data`（见 `src/paths.py`）。下载重试、校验并发等见 `clone.py` 顶部配置。

退出码：有文件仍缺失或校验失败时为 `1`。

### Python API

```python
import asyncio
from src.aws_clone.clone import clone

result = asyncio.run(clone('data/futures/um/daily/klines/*/1m/'))
# result.listed / already_present / to_download / missing_after_download / verified_ok / verified_fail
```

---

## 3. Prefix 语法

- 输入对应列表 API 的 `prefix` 字段（无需带域名）。
- `*` **只能作为完整路径段**，表示「该层所有子目录」。
- 不支持 `**`，也不支持段内部分匹配（如 `BTC*`）。

| 示例 | 含义 |
|------|------|
| `data/futures/um/daily/klines/BTCUSDT/1m/` | 单个 symbol、单个周期 |
| `data/futures/um/daily/klines/*/1m/` | 所有 symbol 的 `1m` |
| `data/futures/um/monthly/fundingRate/` | UM funding 整棵子树 |
| `data/spot/daily/klines/*/1m/` | 现货所有 symbol 的 `1m` |

常见数据前缀形态：

```
data/spot/daily/klines/{symbol}/{interval}/
data/futures/um/daily/klines/{symbol}/{interval}/
data/futures/cm/daily/klines/{symbol}/{interval}/
data/futures/um/monthly/fundingRate/{symbol}/
data/futures/cm/monthly/fundingRate/{symbol}/
```

---

## 4. 流程

```
prefix (可含 *)
    │
    ▼
expand_stars     # async：遇 * 则 list CommonPrefixes 再展开
    │
    ▼
list_keys        # async：对每个具体前缀递归收集 Contents
    │
    ▼
aria2c download  # 对比本地，只下缺失/残缺；已 verified 跳过
    │
    ▼
SHA256 verify    # 仅处理尚无 .verified 的 *.zip
    │
    ▼
CloneResult      # listed / already_present / to_download / …
```

### 下载（增量）

每次跑都会先列远端 key，再和本地对比，**只下缺的**：

| 本地状态 | 行为 |
|----------|------|
| 文件已存在且非空 | 跳过下载 |
| 已有 `.verified` 的 zip | 跳过 zip 与对应 CHECKSUM |
| 远端新增（例如新的一天） | 只下新文件 |
| 校验失败被删掉的坏文件 | 下次重下 |
| 0 字节残缺文件 | 视为缺失，重新下载 |

日志会打印：`remote=… already_present=… to_download=…`。

- 每批最多约 4096 个文件；`aria2c -j32 -x4`。
- 同时拉取 `.zip` 与 `.zip.CHECKSUM`（仅针对缺失项）。

### 校验

- 读 `{file}.CHECKSUM` 第一个 token，与 zip 的 SHA256 比较。
- 成功：创建 `{file}.verified`。
- 失败：删除 zip 与 CHECKSUM（下次 clone 会重新下载）。
- 已有 `.verified` 的 zip 不会再次校验。

### 本地产物示例

```
aws_data/
└── data/futures/um/daily/klines/BTCUSDT/1m/
    ├── BTCUSDT-1m-2024-01-01.zip
    ├── BTCUSDT-1m-2024-01-01.zip.CHECKSUM
    └── BTCUSDT-1m-2024-01-01.zip.verified
```

下游 parse 仍依赖 `.verified` 标记（与旧管线一致）。

---

## 5. 模块结构

```
src/aws_clone/
├── app.py           # Typer CLI（挂到 main.py clone）
├── listing.py       # async 列表 + * 展开
├── clone.py         # 增量下载 + 编排 + 调用校验
├── checksum.py      # SHA256 + .verified
src/paths.py         # 与 aws_parse 共用：BASE_URL / aws_data / Arctic URI
main.py              # 仓库根统一入口（clone / parse）
```

依赖：系统需安装 `aria2c`；Python 侧用项目已有的 `aiohttp`、`xmltodict`、`typer`。

---

## 6. 与旧 `aws/` 的关系

| | 旧 `aws/` | 新 `src/aws_clone` |
|--|-----------|---------------------|
| 输入 | trade_type / interval / symbol 过滤 | 任意 S3 prefix + `*` |
| 下载 | aria2c | 同左 |
| 校验 | `aws/checksum.py` | `src/aws_clone/checksum.py`（语义对齐） |
| 范围 | 按业务类型拆分 CLI | 通用 clone，不做 symbol 过滤 / parse |

新模块 intentionally 不改旧代码；稳定后可逐步用本模块替代旧下载入口。

---

## 7. 注意点

- 通配如 `klines/*/1m/` 会展开成大量 symbol，再列出全部日文件，列表阶段仍可能较慢；下载阶段只会拉差量。
- 重复运行是增量的：已下载跳过，已 verified 不重校；远端新文件会自动补齐。
