"""Fetch current-month funding rates from Binance REST into a pandas frame."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import aiohttp
import pandas as pd

from src.paths import TradeType
from util.log_kit import logger
from util.network import BinanceAPIException, async_retry_getter, create_aiohttp_session

from .store import month_bounds

HTTP_TIMEOUT_SEC = 15

_FUNDING_URL = {
    TradeType.um_futures: 'https://fapi.binance.com/fapi/v1/fundingRate',
    TradeType.cm_futures: 'https://dapi.binance.com/dapi/v1/fundingRate',
}


def _current_month_ms() -> tuple[int, int, int, int]:
    now = datetime.now(timezone.utc)
    start, end = month_bounds(now.year, now.month)
    return now.year, now.month, int(start.timestamp() * 1000), int(end.timestamp() * 1000)


async def _aio_get_json(session: aiohttp.ClientSession, url: str, params: dict) -> list:
    async with session.get(url, params=params) as resp:
        if not str(resp.status).startswith('2'):
            raise BinanceAPIException(resp, resp.status, await resp.text())
        return await resp.json()


def _empty_funding_frame() -> pd.DataFrame:
    return pd.DataFrame({
        'candle_begin_time': pd.Series(dtype='datetime64[ns, UTC]'),
        'symbol': pd.Series(dtype='object'),
        'funding_rate': pd.Series(dtype='float64'),
        'funding_interval_hours': pd.Series(dtype='int64'),
    })


def _rows_to_frame(rows: list, symbol: str) -> pd.DataFrame:
    if not rows:
        return _empty_funding_frame()
    df = pd.DataFrame(rows)
    funding_time = pd.to_numeric(df['fundingTime'], errors='coerce').astype('int64')
    candle_ms = funding_time - funding_time % (60 * 60 * 1000)
    out = pd.DataFrame({
        'candle_begin_time': pd.to_datetime(candle_ms, unit='ms', utc=True),
        'symbol': symbol,
        'funding_rate': pd.to_numeric(df['fundingRate'], errors='coerce').astype('float64'),
    })
    return out


def _fill_funding_interval_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Infer interval hours from consecutive funding times; default 8 when unknown."""
    if df.empty:
        return _empty_funding_frame()
    out = df.sort_values(['symbol', 'candle_begin_time']).copy()
    hours = out.groupby('symbol', sort=False)['candle_begin_time'].diff().dt.total_seconds() / 3600.0
    hours = hours.groupby(out['symbol'], sort=False).bfill().groupby(out['symbol'], sort=False).ffill()
    out['funding_interval_hours'] = hours.fillna(8).round().astype('int64')
    return out.reset_index(drop=True)


async def _fetch_symbol(session: aiohttp.ClientSession, url: str, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    # Wait 75s after a failure because the rate limit is 500/5mins
    data = await async_retry_getter(
        _aio_get_json,
        session=session,
        url=url,
        params={'symbol': symbol, 'startTime': start_ms, 'endTime': end_ms - 1, 'limit': 1000},
        _sleep_seconds=75,
    )
    if data is None:
        return _empty_funding_frame()
    return _rows_to_frame(data, symbol)


async def fetch_current_month_funding(trade_type: str, symbols: list[str]) -> pd.DataFrame:
    """Download current UTC month funding for ``symbols`` from Binance /fundingRate."""
    tt = TradeType(trade_type)
    if tt == TradeType.spot:
        raise RuntimeError('Cannot request funding rate for spot')
    if not symbols:
        return _empty_funding_frame()

    url = _FUNDING_URL[tt]
    year, month, start_ms, end_ms = _current_month_ms()
    logger.info(f'funding-api trade_type={trade_type} month={year:04d}-{month:02d} symbols={len(symbols)}')

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        frames = await asyncio.gather(*[_fetch_symbol(session, url, sym, start_ms, end_ms) for sym in symbols])

    nonempty = [f for f in frames if not f.empty]
    if not nonempty:
        return _empty_funding_frame()

    out = pd.concat(nonempty, ignore_index=True)
    start, end = month_bounds(year, month)
    out = out[(out['candle_begin_time'] >= start) & (out['candle_begin_time'] < end)]
    return _fill_funding_interval_hours(out)


def download_current_month_funding(trade_type: str, symbols: list[str]) -> pd.DataFrame:
    return asyncio.run(fetch_current_month_funding(trade_type, symbols))
