import os
from enum import Enum
from pathlib import Path

BYBIT_DATA_DIR = Path.home() / 'dev' / 'babylake' / 'bybit_data'
BINANCE_DATA_DIR = Path.home() / 'dev' / 'babylake' / 'binance_data'

N_JOBS = int(os.getenv('CRYPTO_NJOBS', os.cpu_count() - 2)) # type: ignore

HTTP_TIMEOUT_SEC = 15

class ExchangeType(str, Enum):
    binance = 'binance'
    bybit = 'bybit'


class TradeType(str, Enum):
    spot = 'spot'
    um_futures = 'um_futures'
    cm_futures = 'cm_futures'


class ContractType(str, Enum):
    perpetual = 'PERPETUAL'
    delivery = 'DELIVERY'


class DataFrequency(Enum):
    """
    Data frequency enumeration class, used to represent the partition type of data.

    Enum values:
        yearly: Yearly partition, indicating data is divided by year.
        monthly: Monthly partition, indicating data is divided by month.
        daily: Daily partition, indicating data is divided by day.
    """
    yearly = 'yearly'  # Yearly partition, data divided by year
    monthly = 'monthly'  # Monthly partition, data divided by month
    daily = 'daily'  # Daily partition, data divided by day
