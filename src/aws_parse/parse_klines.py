"""Step1a: verified daily kline zips -> Arctic ``klines_{interval}``."""

from __future__ import annotations

import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import polars as pl
from tqdm import tqdm

from src.paths import (
    DEFAULT_ARCTIC_URI,
    DEFAULT_AWS_DATA_DIR,
    TradeType,
    arctic_klines_symbol,
    is_verified_zip,
    klines_root,
    parse_kline_zip_date,
)
from util.concurrent import mp_env_init
from util.log_kit import logger

from .store import (
    day_bounds,
    get_library,
    has_data_in_range,
    to_multiindex_df,
    update_multiindex,
)


def read_kline_csv(csv_file: Path) -> pl.DataFrame:
    columns = [
        'candle_begin_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'close_time',
        'quote_volume',
        'trade_num',
        'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume',
        'ignore',
    ]
    schema = {
        'candle_begin_time': pl.Int64,
        'open': pl.Float64,
        'high': pl.Float64,
        'low': pl.Float64,
        'close': pl.Float64,
        'volume': pl.Float64,
        'quote_volume': pl.Float64,
        'trade_num': pl.Int64,
        'taker_buy_base_asset_volume': pl.Float64,
        'taker_buy_quote_asset_volume': pl.Float64,
    }
    with ZipFile(csv_file) as f:
        filename = f.namelist()[0]
        lines = f.open(filename).readlines()
        if lines and lines[0].decode().startswith('open_time'):
            lines = lines[1:]

    ldf = pl.scan_csv(lines, has_header=False, new_columns=columns, schema_overrides=schema)
    ldf = ldf.drop('ignore', 'close_time')
    df = ldf.collect()

    ts_unit = 'ms'
    if df['candle_begin_time'].max() >= (10**15):  # type: ignore
        ts_unit = 'us'

    return df.with_columns(
        pl.col('candle_begin_time').cast(pl.Datetime(ts_unit)).dt.replace_time_zone('UTC').dt.cast_time_unit('ms')
    )


def read_kline_zip_pandas(zip_path: Path, symbol: str) -> pd.DataFrame:
    df = read_kline_csv(zip_path).to_pandas()
    df['symbol'] = symbol
    return df


def _read_one(args: tuple[str, str]) -> pd.DataFrame:
    zip_path, symbol = args
    return read_kline_zip_pandas(Path(zip_path), symbol)


def collect_kline_zips_by_day(aws_data_dir: Path, trade_type: str, interval: str, symbols: list[str] | None = None) -> dict[date, list[tuple[Path, str]]]:
    tt = TradeType(trade_type)
    root = klines_root(aws_data_dir, tt)
    by_day: dict[date, list[tuple[Path, str]]] = defaultdict(list)
    if not root.exists():
        return by_day

    symbol_filter = set(symbols) if symbols else None
    for symbol_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        symbol = symbol_dir.name
        if symbol_filter is not None and symbol not in symbol_filter:
            continue
        interval_dir = symbol_dir / interval
        if not interval_dir.is_dir():
            continue
        for zip_path in interval_dir.glob('*.zip'):
            if not is_verified_zip(zip_path):
                continue
            try:
                day = parse_kline_zip_date(zip_path)
            except ValueError:
                logger.warning(f'skip unparseable kline zip: {zip_path}')
                continue
            by_day[day].append((zip_path, symbol))
    return dict(sorted(by_day.items()))


def parse_klines(trade_type: str, interval: str, *, aws_data_dir: Path = DEFAULT_AWS_DATA_DIR, arctic_uri: str = DEFAULT_ARCTIC_URI, force: bool = False, symbols: list[str] | None = None, n_jobs: int | None = None) -> dict[str, int]:
    """Ingest verified kline zips into ArcticDB, one day cross-section per update."""
    if n_jobs is None:
        n_jobs = max(1, (os.cpu_count() or 2) - 2)

    lib = get_library(trade_type, arctic_uri)
    arctic_sym = arctic_klines_symbol(interval)
    by_day = collect_kline_zips_by_day(aws_data_dir, trade_type, interval, symbols)

    skipped = 0
    updated = 0
    rows = 0

    logger.info(f'parse-klines trade_type={trade_type} interval={interval} days={len(by_day)} force={force}')

    for day, files in tqdm(by_day.items(), desc='parse-klines', ncols=100):
        # ================================================
        # 1. skip days already present (unless force)
        # ================================================
        start, end = day_bounds(day)
        if not force and has_data_in_range(lib, arctic_sym, start, end):
            skipped += 1
            continue

        # ================================================
        # 2. parallel read zip -> concat day cross-section
        # ================================================
        frames = []
        with ProcessPoolExecutor(max_workers=n_jobs, initializer=mp_env_init) as exe:
            futs = [exe.submit(_read_one, (str(p), sym)) for p, sym in files]
            for fut in as_completed(futs):
                frames.append(fut.result())

        if not frames:
            continue

        day_df = to_multiindex_df(pd.concat(frames, ignore_index=True))
        rows += update_multiindex(lib, arctic_sym, day_df, date_range=(start, end), replace_symbols=symbols)
        updated += 1

    logger.info(f'parse-klines done updated_days={updated} skipped_days={skipped} rows={rows}')
    return {'updated_days': updated, 'skipped_days': skipped, 'rows': rows}
