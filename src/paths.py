"""Shared paths and layout helpers for src/ aws_clone + aws_parse."""

from datetime import date
from enum import Enum
from pathlib import Path


class TradeType(str, Enum):
    spot = 'spot'
    um_futures = 'um_futures'
    cm_futures = 'cm_futures'


BASE_URL = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
ROOT_DIR = Path.home() / 'dev' / 'babylake'
DEFAULT_AWS_DATA_DIR = ROOT_DIR / 'binance_data' / 'aws_data'
DEFAULT_ARCTIC_URI = f'lmdb://{ROOT_DIR / "arctic"}'

_TRADE_TYPE_PREFIX = {
    TradeType.spot: Path('data') / 'spot',
    TradeType.um_futures: Path('data') / 'futures' / 'um',
    TradeType.cm_futures: Path('data') / 'futures' / 'cm',
}


def verified_marker(zip_path: Path) -> Path:
    return zip_path.parent / (zip_path.name + '.verified')


def is_verified_zip(zip_path: Path) -> bool:
    return zip_path.is_file() and verified_marker(zip_path).exists()


def trade_type_prefix(trade_type: TradeType) -> Path:
    return _TRADE_TYPE_PREFIX[trade_type]


def klines_root(aws_data_dir: Path, trade_type: TradeType) -> Path:
    return aws_data_dir / trade_type_prefix(trade_type) / 'daily' / 'klines'


def funding_root(aws_data_dir: Path, trade_type: TradeType) -> Path:
    return aws_data_dir / trade_type_prefix(trade_type) / 'monthly' / 'fundingRate'


def parse_kline_zip_date(zip_path: Path) -> date:
    """BTCUSDT-1m-2024-01-01.zip -> date(2024, 1, 1)."""
    parts = zip_path.stem.split('-')
    if len(parts) < 3:
        raise ValueError(f'Cannot parse date from kline zip name: {zip_path.name}')
    y, m, d = parts[-3], parts[-2], parts[-1]
    return date(int(y), int(m), int(d))


def parse_funding_zip_month(zip_path: Path) -> tuple[int, int]:
    """BTCUSDT-fundingRate-2024-01.zip -> (2024, 1)."""
    parts = zip_path.stem.split('-')
    if len(parts) < 2:
        raise ValueError(f'Cannot parse month from funding zip name: {zip_path.name}')
    y, m = parts[-2], parts[-1]
    return int(y), int(m)


def arctic_klines_symbol(interval: str) -> str:
    return f'klines_{interval}'


def arctic_funding_symbol() -> str:
    return 'funding'


def arctic_wide_symbol(interval: str) -> str:
    return f'klines_{interval}_wide'
