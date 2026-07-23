"""Step1b: verified monthly funding zips -> Arctic ``funding``."""

from __future__ import annotations

import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import polars as pl
from tqdm import tqdm

from config import TradeType
from src.paths import (
    DEFAULT_ARCTIC_URI,
    DEFAULT_AWS_DATA_DIR,
    arctic_funding_symbol,
    funding_root,
    is_verified_zip,
    klines_root,
    parse_funding_zip_month,
)
from util.concurrent import mp_env_init
from util.log_kit import logger

from .funding_api import download_current_month_funding
from .store import (
    get_library,
    has_data_in_range,
    month_bounds,
    to_multiindex_df,
    update_multiindex,
)


def read_funding_csv(funding_file: Path) -> pl.DataFrame:
    with ZipFile(funding_file) as f:
        filename = f.namelist()[0]
        lines = f.open(filename).readlines()
    if lines and lines[0].decode().startswith('calc_time'):
        lines = lines[1:]

    columns = ['funding_time', 'funding_interval_hours', 'funding_rate']
    schema = {
        'funding_time': pl.Int64,
        'funding_interval_hours': pl.Int64,
        'funding_rate': pl.Float64,
    }
    ldf = pl.scan_csv(lines, has_header=False, new_columns=columns, schema_overrides=schema)
    ldf = ldf.with_columns(
        (pl.col('funding_time') - pl.col('funding_time') % (60 * 60 * 1000)).alias('candle_begin_time')
    )
    ldf = ldf.with_columns(
        pl.col('candle_begin_time').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC'),
        pl.col('funding_time').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC'),
    )
    return ldf.collect()


def read_funding_zip_pandas(zip_path: Path, symbol: str) -> pd.DataFrame:
    df = read_funding_csv(zip_path).to_pandas()
    df['symbol'] = symbol
    return df[['candle_begin_time', 'symbol', 'funding_rate', 'funding_interval_hours']]


def _read_one(args: tuple[str, str]) -> pd.DataFrame:
    zip_path, symbol = args
    return read_funding_zip_pandas(Path(zip_path), symbol)


def collect_funding_zips_by_month(aws_data_dir: Path, trade_type: str, symbols: list[str] | None = None) -> dict[tuple[int, int], list[tuple[Path, str]]]:
    tt = TradeType(trade_type)
    root = funding_root(aws_data_dir, tt)
    by_month: dict[tuple[int, int], list[tuple[Path, str]]] = defaultdict(list)
    if not root.exists():
        return by_month

    symbol_filter = set(symbols) if symbols else None
    for symbol_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        symbol = symbol_dir.name
        if symbol_filter is not None and symbol not in symbol_filter:
            continue
        for zip_path in symbol_dir.glob('*.zip'):
            if not is_verified_zip(zip_path):
                continue
            try:
                ym = parse_funding_zip_month(zip_path)
            except ValueError:
                logger.warning(f'skip unparseable funding zip: {zip_path}')
                continue
            by_month[ym].append((zip_path, symbol))
    return dict(sorted(by_month.items()))


def resolve_funding_symbols(aws_data_dir: Path, trade_type: str, symbols: list[str] | None = None) -> list[str]:
    if symbols is not None:
        return list(symbols)

    tt = TradeType(trade_type)
    kroot = klines_root(aws_data_dir, tt)
    if kroot.exists():
        found = sorted(p.name for p in kroot.iterdir() if p.is_dir() and (p / '1m').is_dir())
        if found:
            return found

    froot = funding_root(aws_data_dir, tt)
    if froot.exists():
        return sorted(p.name for p in froot.iterdir() if p.is_dir())
    return []


def parse_funding(trade_type: str, *, aws_data_dir: Path = DEFAULT_AWS_DATA_DIR, arctic_uri: str = DEFAULT_ARCTIC_URI, force: bool = False, symbols: list[str] | None = None, n_jobs: int | None = None) -> dict[str, int]:
    """Ingest verified funding zips into ArcticDB, one month cross-section per update."""
    if trade_type == 'spot':
        logger.info('parse-funding skipped for spot (no fundingRate)')
        return {'updated_months': 0, 'skipped_months': 0, 'rows': 0, 'api_updated': 0, 'api_rows': 0}

    if n_jobs is None:
        n_jobs = max(1, (os.cpu_count() or 2) - 2)

    lib = get_library(trade_type, arctic_uri)
    arctic_sym = arctic_funding_symbol()
    by_month = collect_funding_zips_by_month(aws_data_dir, trade_type, symbols)

    skipped = 0
    updated = 0
    rows = 0
    api_updated = 0
    api_rows = 0

    logger.info(f'parse-funding trade_type={trade_type} months={len(by_month)} force={force}')

    for (year, month), files in tqdm(by_month.items(), desc='parse-funding', ncols=100):
        # ================================================
        # 1. skip months already present (unless force)
        # ================================================
        start, end = month_bounds(year, month)
        if not force and has_data_in_range(lib, arctic_sym, start, end):
            skipped += 1
            continue

        # ================================================
        # 2. parallel read zip -> concat month cross-section
        # ================================================
        frames = []
        with ProcessPoolExecutor(max_workers=n_jobs, initializer=mp_env_init) as exe:
            futs = [exe.submit(_read_one, (str(p), sym)) for p, sym in files]
            for fut in as_completed(futs):
                frames.append(fut.result())

        if not frames:
            continue

        month_df = to_multiindex_df(pd.concat(frames, ignore_index=True))
        rows += update_multiindex(lib, arctic_sym, month_df, date_range=(start, end), replace_symbols=symbols)
        updated += 1

    # ================================================
    # 3. current UTC month from Binance API (AWS has no monthly zip yet)
    # ================================================
    api_symbols = resolve_funding_symbols(aws_data_dir, trade_type, symbols)
    if api_symbols:
        now = datetime.now(timezone.utc)
        start, end = month_bounds(now.year, now.month)
        api_df = download_current_month_funding(trade_type, api_symbols)
        if not api_df.empty:
            month_df = to_multiindex_df(api_df)
            api_rows = update_multiindex(lib, arctic_sym, month_df, date_range=(start, end), replace_symbols=symbols)
            api_updated = 1
            rows += api_rows
        else:
            logger.warning(f'parse-funding api returned empty for {now.year:04d}-{now.month:02d}')
    else:
        logger.warning('parse-funding api skipped: no symbols resolved')

    logger.info(f'parse-funding done updated_months={updated} skipped_months={skipped} rows={rows} api_updated={api_updated} api_rows={api_rows}')
    return {'updated_months': updated, 'skipped_months': skipped, 'rows': rows, 'api_updated': api_updated, 'api_rows': api_rows}

