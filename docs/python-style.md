# Python 代码风格

> 只管"代码怎么写". 项目结构, 改策略流程, 模块职责见 [AGENTS.md](../AGENTS.md).

新增或修改代码时, 与现有 `src/` 保持一致; 不要按通用 PEP8/formatter 模板"美化"已有写法.

## 标点符号

**所有地方** (`.py` 源码, 注释, docstring, 日志, 文档, commit message 等) 即使用中文, **标点也必须用英文半角**, 不要用中文全角标点.

| 用 | 不用 |
|----|------|
| `,` `.` `;` `:` `()` | `，` `。` `；` `：` `（）` |

```python
# ✅ 选币完成, 共 3 个多头
# ❌ 选币完成，共 3 个多头

logger.warning('delisting symbols: {}', symbols)   # ✅
logger.warning('delisting symbols：{}', symbols)   # ❌

pusher.send_text(f'{acct.name} 资金利用率: {lev:.0%}')  # ✅
pusher.send_text(f'{acct.name} 资金利用率：{lev:.0%}')  # ❌
```

顺手改到旧代码里的中文标点时, 只改标点, 不改措辞.

## 导入

stdlib → 第三方 → `from src.xxx`; 组间空一行; 用绝对导入.

```python
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from loguru import logger

from src.trader_bn.modules.factors import Factor, Filters
```

## 类型注解

Python 3.12 写法: `list[str]`, `dict | None`, 返回值标 `-> None` / `-> pd.DataFrame`.

## 函数定义

参数写在同一行, 不按参数换行; 即使签名较长也保持单行.

```python
# ✅
def get_select_coin(self, df: pd.DataFrame, delist_long: list[str], delist_short: list[str]) -> pd.DataFrame:

def trade(self, ready_data: pd.DataFrame, delist_long: list[str], delist_short: list[str], min_qty: dict, price_precision: dict, min_notional: dict, debug: bool = True) -> None:

# ❌ 不要拆成多行参数
def trade(
    self,
    ready_data: pd.DataFrame,
    delist_long: list[str],
    ...
) -> None:
```

## 字符串与字面量

- 字符串优先单引号 `'...'`
- 数值可用下划线: `25_000`
- f-string 用于动态列名: `f'{factor_name}_bh_{bh}'`

## dataclass

- 配置类用 `@dataclass`; 可变默认用 `field(default_factory=...)`
- 字段冒号两侧空格, 值侧对齐; cfg 区块用 `# ============================================================` 分隔
- 初始化校验放 `__post_init__`

```python
@dataclass
class Strategy:
    # ============================================================
    # cfg -- factor
    # ============================================================
    factor_long     : Factor
    select_long_num : float
    black_list      : list = field(default_factory=list)
```

配置 dict / 列表命名: `mtm_args`, `strategies_mtm`.

## 注释与分段

- 长函数内用 `# ================================================` 分步, 可编号 (`# 1. 选币`)
- 业务逻辑注释用中文; 模块级 docstring 可用 `"""` 或 `'''`
- 能自解释就不写注释; 只写非显然的业务规则

```python
def trade(self, ready_data: pd.DataFrame, ...) -> None:
    # ================================================
    # 1. 选币 + 计算目标持仓量
    # ================================================
    ...
```

## 命名

| 类别 | 风格 | 示例 |
|------|------|------|
| 类 | PascalCase | `StrategyTrader`, `SubTraderAcct` |
| 函数/变量 | snake_case | `get_select_coin`, `select_long_num` |
| 常量 | UPPER_SNAKE | `KEEP_COLS`, `NON_TRADE_SYMBOLS` |
| DataFrame 列 | 中文或英文 | `方向`, `目标持仓量`, `candle_begin_time`, `symbol` |
| 因子列 | `{name}_bh_{n}` | `mtm_cj_v11_bh_25` |

## Pandas / NumPy

- 条件赋值: `df.loc[cond, col] = val`; 缺失: `np.nan`
- 按时间分组: `groupby('candle_begin_time', sort=False)`
- 链式赋值保持现有风格, 不强行统一运算符两侧空格
- pandas 类型告警可 `# type: ignore`, 不为消告警大改结构

## 惯用写法

```python
# 日志: loguru 占位符, 不用 f-string
logger.info('order details for acct {}: {}', self.name, data)

# API 调用
err_retry(self.exchange.papi_get_um_account)(params={'timestamp': ''})
```

## 不要做的"风格改动"

- 不顺手统一全文件的引号, 空格, 换行
- 不引入与现有文件明显不同的抽象层或 helper
- 不把中文列名改成英文"为了规范"
