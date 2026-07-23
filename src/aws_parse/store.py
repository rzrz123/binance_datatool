"""ArcticDB connection and MultiIndex update helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
from arcticdb import Arctic
from arcticdb.version_store.library import Library

from src.paths import DEFAULT_ARCTIC_URI

INDEX_NAMES = ('candle_begin_time', 'symbol')


def get_library(trade_type: str, uri: str = DEFAULT_ARCTIC_URI) -> Library:
    ac = Arctic(uri)
    return ac.get_library(trade_type, create_if_missing=True)


def to_multiindex_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure MultiIndex (candle_begin_time, symbol), sorted, UTC timestamps."""
    if not isinstance(df.index, pd.MultiIndex) or list(df.index.names) != list(INDEX_NAMES):
        work = df.reset_index() if isinstance(df.index, pd.MultiIndex) else df.copy()
        if 'candle_begin_time' not in work.columns or 'symbol' not in work.columns:
            raise ValueError(f'DataFrame must have columns {INDEX_NAMES}, got {list(work.columns)}')
        work['candle_begin_time'] = pd.to_datetime(work['candle_begin_time'], utc=True)
        work = work.set_index(list(INDEX_NAMES))
    else:
        work = df.copy()
        level0 = pd.to_datetime(work.index.get_level_values(0), utc=True)
        level1 = work.index.get_level_values(1)
        work.index = pd.MultiIndex.from_arrays([level0, level1], names=INDEX_NAMES)

    if work.index.has_duplicates:
        work = work[~work.index.duplicated(keep='last')]
    return work.sort_index()


def day_bounds(day: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(datetime(day.year, day.month, day.day, tzinfo=timezone.utc))
    end = start + pd.Timedelta(days=1)
    return start, end


def month_bounds(year: int, month: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(datetime(year, month, 1, tzinfo=timezone.utc))
    if month == 12:
        end = pd.Timestamp(datetime(year + 1, 1, 1, tzinfo=timezone.utc))
    else:
        end = pd.Timestamp(datetime(year, month + 1, 1, tzinfo=timezone.utc))
    return start, end


def has_data_in_range(lib: Library, symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    if not lib.has_symbol(symbol):
        return False
    result = lib.read(symbol, date_range=(start, end - pd.Timedelta(nanoseconds=1)))
    data = result.data
    return data is not None and not data.empty


def read_range(lib: Library, symbol: str, start: pd.Timestamp, end: pd.Timestamp, columns: list[str] | None = None) -> pd.DataFrame:
    if not lib.has_symbol(symbol):
        return pd.DataFrame()
    result = lib.read(symbol, date_range=(start, end - pd.Timedelta(nanoseconds=1)), columns=columns)
    data = result.data
    if data is None or data.empty:
        return pd.DataFrame()
    return to_multiindex_df(data)


def update_multiindex(lib: Library, symbol: str, df: pd.DataFrame, *, date_range: tuple[pd.Timestamp, pd.Timestamp] | None = None, replace_symbols: list[str] | None = None) -> int:
    """Replace the datetime span of ``df`` (outermost index) via ArcticDB update.

    If ``replace_symbols`` is set, keep existing rows for symbols *outside* that set
    in ``date_range`` so a partial-symbol ingest does not wipe the rest of the day/month.
    Returns number of rows written.
    """
    payload = to_multiindex_df(df)
    if replace_symbols is not None:
        if date_range is None:
            raise ValueError('replace_symbols requires date_range')
        start, end = date_range
        existing = read_range(lib, symbol, start, end)
        if not existing.empty:
            keep = existing[~existing.index.get_level_values('symbol').isin(replace_symbols)]
            if not keep.empty:
                payload = to_multiindex_df(pd.concat([keep, payload]))

    if payload.empty:
        return 0
    kwargs = {'upsert': True}
    if date_range is not None:
        start, end = date_range
        # Arctic date_range end is inclusive-ish; pass half-open via end-1ns
        kwargs['date_range'] = (start, end - pd.Timedelta(nanoseconds=1))
    lib.update(symbol, payload, **kwargs)
    return len(payload)
