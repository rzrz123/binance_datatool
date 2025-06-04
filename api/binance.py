import asyncio
import json
from datetime import datetime, timedelta
from datetime import time as dtime
from decimal import Decimal
from abc import ABC, abstractmethod
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from pathlib import Path
import aiohttp
import polars as pl
from tqdm import tqdm
from dateutil import parser as date_parser

from config import TradeType, BINANCE_DATA_DIR, HTTP_TIMEOUT_SEC
from aws.kline.util import local_list_kline_symbols
from util.log_kit import logger
from util.network import create_aiohttp_session, async_retry_getter
from util.time import async_sleep_until_run_time, convert_date, convert_interval_to_timedelta, next_run_time
from util.ts_manager import TSManager


class BinanceRequestException(Exception):

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'BinanceRequestException: %s' % self.message


class BinanceAPIException(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res.get('code')
            self.message = json_res.get('msg')
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class BinanceBaseApi:

    def __init__(self, session: aiohttp.ClientSession, proxy: str) -> None:
        self.session = session
        self.proxy = proxy

    async def _handle_response(self, response: aiohttp.ClientResponse):
        """
        Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the response.
        """
        if not str(response.status).startswith('2'):
            raise BinanceAPIException(response, response.status, await response.text())
        try:
            return await response.json()
        except ValueError:
            txt = await response.text()
            raise BinanceRequestException(f'Invalid Response: {txt}')

    async def _aio_get(self, url, params):
        if params is None:
            params = {}
        async with self.session.get(url, params=params, proxy=self.proxy) as resp:
            return await self._handle_response(resp)

    async def _aio_post(self, url, params):
        async with self.session.post(url, data=params, proxy=self.proxy) as resp:
            return await self._handle_response(resp)


class BinanceBaseMarketApi(ABC, BinanceBaseApi):
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1000

    @abstractmethod
    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        pass

    @abstractmethod
    async def aioreq_klines(self, **kwargs) -> list:
        pass

    @abstractmethod
    async def aioreq_exchange_info(self) -> dict:
        pass

    async def aioreq_premium_index(self, **kwargs) -> list:
        raise NotImplementedError


class BinanceMarketUMFapi(BinanceBaseMarketApi):
    """
    Abstraction for binance USDⓈ-M Futures Fapi market endpoints
    """

    PREFIX = 'https://fapi.binance.com/fapi'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1500

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v1/time'
        async with self.session.get(url, proxy=self.proxy) as resp:
            weight = int(resp.headers['X-MBX-USED-WEIGHT-1M'])
            timestamp = (await resp.json())['serverTime']
        return timestamp, weight

    async def aioreq_klines(self, **kwargs) -> list:
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f'{self.PREFIX}/v1/klines'
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self) -> dict:
        """
        Get current exchange trading rules and symbol information
        """
        url = f'{self.PREFIX}/v1/exchangeInfo'
        return await self._aio_get(url, None)

    async def aioreq_premium_index(self, **kwargs) -> list:
        """
        Get Mark Price and Funding Rate
        """
        url = f'{self.PREFIX}/v1/premiumIndex'
        return await self._aio_get(url, kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        """
        Get Funding Rate History
        """
        url = f'{self.PREFIX}/v1/fundingRate'
        return await self._aio_get(url, kwargs)

    async def aioreq_book_ticker(self, **kwargs):
        """
        Best price/qty on the order book for a symbol or symbols.
        """
        url = f'{self.PREFIX}/v1/ticker/bookTicker'
        return await self._aio_get(url, kwargs)


class BinanceMarketCMDapi(BinanceBaseMarketApi):
    """
    Abstraction for Binance COIN-M Futures Dapi market endpoints
    """

    PREFIX = 'https://dapi.binance.com/dapi'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1500

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v1/time'
        async with self.session.get(url, proxy=self.proxy) as resp:
            weight = int(resp.headers['X-MBX-USED-WEIGHT-1M'])
            timestamp = (await resp.json())['serverTime']
        return timestamp, weight

    async def aioreq_klines(self, **kwargs) -> list:
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f'{self.PREFIX}/v1/klines'
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self) -> dict:
        """
        Get Current exchange trading rules and symbol information
        """
        url = f'{self.PREFIX}/v1/exchangeInfo'
        return await self._aio_get(url, None)

    async def aioreq_premium_index(self, **kwargs) -> list:
        """
        Get Mark Price and Funding Rate
        """
        url = f'{self.PREFIX}/v1/premiumIndex'
        return await self._aio_get(url, kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        """
        Get Funding Rate History
        """
        url = f'{self.PREFIX}/v1/fundingRate'
        return await self._aio_get(url, kwargs)


class BinanceMarketSpotApi(BinanceBaseMarketApi):
    """
    Abstraction for Binance Spot Api market endpoints
    """

    PREFIX = 'https://api.binance.com/api'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 1000
    MAX_MINUTE_WEIGHT = 6000
    MAX_ONCE_CANDLES = 1000

    async def aioreq_time_and_weight(self) -> Tuple[int, int]:
        """
        Get the current server time and consumed weight
        """
        url = f'{self.PREFIX}/v3/time'
        async with self.session.get(url, proxy=self.proxy) as resp:
            weight = int(resp.headers['X-MBX-USED-WEIGHT-1M'])
            timestamp = (await resp.json())['serverTime']
        return timestamp, weight

    async def aioreq_klines(self, **kwargs):
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        url = f'{self.PREFIX}/v3/klines'
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self):
        """
        Get Current exchange trading rules and symbol information
        """
        url = f'{self.PREFIX}/v3/exchangeInfo'
        return await self._aio_get(url, None)


def create_binance_market_api(trade_type: TradeType, session, http_proxy) -> BinanceBaseMarketApi:
    match trade_type:
        case TradeType.spot:
            return BinanceMarketSpotApi(session, http_proxy)
        case TradeType.um_futures:
            return BinanceMarketUMFapi(session, http_proxy)
        case TradeType.cm_futures:
            return BinanceMarketCMDapi(session, http_proxy)


def _get_from_filters(filters, filter_type, field_name):
    for f in filters:
        if f['filterType'] == filter_type:
            return f[field_name]


def _parse_um_futures_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(_get_from_filters(filters, 'LOT_SIZE', 'stepSize')),
        'min_notional_value': Decimal(_get_from_filters(filters, 'MIN_NOTIONAL', 'notional'))
    }


def _parse_cm_futures_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['contractStatus'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(info['contractSize'])
    }


def _parse_spot_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(_get_from_filters(filters, 'LOT_SIZE', 'stepSize')),
        'min_notional_value': Decimal(_get_from_filters(filters, 'NOTIONAL', 'minNotional'))
    }


class BinanceFetcher:

    TYPE_MAP = {
        TradeType.um_futures: _parse_um_futures_syminfo,
        TradeType.cm_futures: _parse_cm_futures_syminfo,
        TradeType.spot: _parse_spot_syminfo,
    }

    def __init__(self, trade_type: TradeType, session: aiohttp.ClientSession, http_proxy=None):
        self.trade_type = trade_type
        self.market_api = create_binance_market_api(trade_type, session, http_proxy)

        if trade_type in self.TYPE_MAP:
            self.syminfo_parse_func = self.TYPE_MAP[trade_type]
        else:
            raise ValueError(f'Type {trade_type} not supported')

    def get_api_limits(self) -> tuple[int, int]:
        return self.market_api.MAX_MINUTE_WEIGHT, self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES

    async def get_time_and_weight(self) -> tuple[datetime, int]:
        server_timestamp, weight = await self.market_api.aioreq_time_and_weight()
        server_timestamp = datetime.fromtimestamp(server_timestamp / 1000, ZoneInfo('UTC'))
        return server_timestamp, weight

    async def get_exchange_info(self) -> dict[str, dict]:
        """
        Parse trading rules from return values of /exchangeinfo API
        """
        exg_info = await async_retry_getter(self.market_api.aioreq_exchange_info)
        results = dict()
        for info in exg_info['symbols']:
            results[info['symbol']] = self.syminfo_parse_func(info)
        return results

    async def get_kline_df(self, symbol, interval, **kwargs) -> Optional[pl.DataFrame]:
        '''
        Request and parse return values of /klines API and convert to polars.DataFrame
        '''
        klines = await async_retry_getter(self.market_api.aioreq_klines, symbol=symbol, interval=interval, **kwargs)
        if klines is None:
            return None

        columns = [
            'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
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
            'taker_buy_quote_asset_volume': pl.Float64
        }
        lf = pl.LazyFrame(klines, schema=columns, orient='row', schema_overrides=schema)
        lf = lf.drop('close_time', 'ignore')
        lf = lf.with_columns(pl.col('candle_begin_time').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC'))
        lf = lf.unique('candle_begin_time', keep='last')
        lf = lf.sort('candle_begin_time')
        df = lf.collect()
        return df

    async def get_kline_df_of_day(self, symbol, interval, dt) -> Optional[pl.DataFrame]:
        '''
        Request and parse return values of /klines API of given date and convert to polars.DataFrame
        '''
        if isinstance(dt, str):
            dt = date_parser.parse(dt).date()
        ts_start = datetime.combine(dt, dtime(0, 0), tzinfo=ZoneInfo('UTC'))
        ts_next = ts_start + timedelta(days=1)
        start_ms = int(ts_start.timestamp()) * 1000
        noon_ms = int(datetime.combine(dt, dtime(12, 0), tzinfo=ZoneInfo('UTC')).timestamp()) * 1000
        end_ms = int(ts_next.timestamp()) * 1000 - 1

        max_once_candles = self.market_api.MAX_ONCE_CANDLES
        num = timedelta(days=1) // convert_interval_to_timedelta(interval)

        if num <= max_once_candles:
            result = await self.get_kline_df(symbol, interval, startTime=start_ms, endTime=end_ms, limit=max_once_candles)
            lf = result.lazy() if result is not None else None
        else:
            results = await asyncio.gather(
                self.get_kline_df(symbol, interval, startTime=start_ms, endTime=noon_ms, limit=max_once_candles),
                self.get_kline_df(symbol, interval, startTime=noon_ms, endTime=end_ms, limit=max_once_candles))
            results = [r.lazy() for r in results if r is not None]
            lf = pl.concat(results) if results else None

        if lf is None:
            return None

        lf = lf.unique('candle_begin_time', keep='last')
        lf = lf.filter(pl.col('candle_begin_time').is_between(ts_start, ts_next, 'left'))
        lf = lf.sort('candle_begin_time')
        df = lf.collect()
        
        return df

    async def get_realtime_funding_rate(self) -> pl.DataFrame:
        if self.trade_type == TradeType.spot:
            raise RuntimeError('Cannot request funding rate for spot')
        data = await async_retry_getter(self.market_api.aioreq_premium_index)
        schema = {
            'nextFundingTime': pl.Int64,
            'lastFundingRate': pl.Float64,
            'symbol': str
        }
        df = pl.LazyFrame(data, schema_overrides=schema, orient='row')
        df = df.filter(pl.col('nextFundingTime') > 0)
        df = df.select(
            pl.col('nextFundingTime').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC').alias('next_funding_time'),
            pl.col('symbol'),
            pl.col('lastFundingRate').alias('funding_rate')
        )
        return df.collect()

    async def get_hist_funding_rate(self, symbol, **kwargs):
        '''
        Parse historical funding rates from /fundingRate
        '''
        if self.trade_type == 'spot':
            raise RuntimeError('Cannot request funding rate for spot')

        # Wait for 75s after a failure because the rate limit is 500/5mins
        data = await async_retry_getter(self.market_api.aioreq_funding_rate, symbol=symbol, _sleep_seconds=75, **kwargs)
        schema = {
            'fundingTime': pl.Int64,
            'fundingRate': pl.Float64,
            'symbol': str
        }
        df = pl.LazyFrame(data, orient='row', schema_overrides=schema)

        candle_begin_time = pl.col('fundingTime') - pl.col('fundingTime') % (60 * 60 * 1000)
        df = df.select(
            pl.col('fundingTime').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC').alias('funding_time'),
            candle_begin_time.cast(pl.Datetime('ms')).dt.replace_time_zone('UTC').alias('candle_begin_time'),
            pl.col('fundingRate').alias('funding_rate')
        )
        df = df.sort(['candle_begin_time'])
        return df.collect()


async def download_funding_for_symbol(funding_dir: Path, symbol: str, fetcher: BinanceFetcher):
    df_funding = await fetcher.get_hist_funding_rate(symbol=symbol, limit=1000)

    output_file = funding_dir / f"{symbol}.pqt"
    df_funding.write_parquet(output_file)


async def download_funding_rates(trade_type: TradeType, symbols: list[str], http_proxy: Optional[str]):
    logger.debug(f"Start Download {trade_type.value} {symbols[0]} -- {symbols[-1]} Funding Rates from Binance API")

    funding_dir = BINANCE_DATA_DIR / "api_data" / trade_type.value / "funding_rate"
    funding_dir.mkdir(parents=True, exist_ok=True)

    logger.debug(f"Funding rate directory: {funding_dir}")

    if http_proxy is not None:
        logger.debug(f"Use proxy, http_proxy={http_proxy}")

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session, http_proxy)
        tasks = [download_funding_for_symbol(funding_dir, symbol, fetcher) for symbol in symbols]
        await asyncio.gather(*tasks)

    logger.debug(f"{trade_type.value} {symbols[0]} -- {symbols[-1]} API Funding Rates download successfully")


async def download_funding_rates_all(trade_type: TradeType, http_proxy: Optional[str]):
    logger.info(f"BHDS Recent {trade_type.value} Funding Rates API Download")
    symbols = local_list_kline_symbols(trade_type, "1m")
    await download_funding_rates(trade_type, symbols, http_proxy)


async def _get_kline(fetcher: BinanceFetcher, symbol: str, time_interval: str, dt: str):
    df = await fetcher.get_kline_df_of_day(symbol, time_interval, dt)
    return df, symbol, dt


async def api_download_kline(
    trade_type: TradeType,
    time_interval: str,
    sym_dts: list[tuple[str, str]],
    http_proxy: Optional[str],
):
    BATCH_SIZE = 40

    logger.debug(f"Start Download {trade_type.value} {time_interval} {len(sym_dts)} Klines from Binance API")
    if http_proxy is not None:
        logger.debug(f"Use proxy, http_proxy={http_proxy}")
    sym_dts = sorted([(sym, convert_date(dt)) for sym, dt in sym_dts])  # type: ignore

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session, http_proxy)
        while sym_dts:
            server_ts, weight = await fetcher.get_time_and_weight()
            batch, sym_dts = sym_dts[:BATCH_SIZE], sym_dts[BATCH_SIZE:]
            logger.debug(f"server_time={server_ts}, weight_used={weight}, start={batch[0]}, end={batch[-1]}")

            max_minute_weight, _ = fetcher.get_api_limits()
            if weight > max_minute_weight - 480:
                logger.info(f"Weight {weight} exceeds the maximum limit, sleep until next minute")
                await async_sleep_until_run_time(next_run_time("1m"))
                continue

            tasks = [asyncio.create_task(_get_kline(fetcher, sym, time_interval, dt)) for sym, dt in batch]

            for task in asyncio.as_completed(tasks):
                df, symbol, dt = await task
                if df is None:
                    continue
                filename = dt.strftime("%Y%m%d") + ".pqt" # type: ignore
                kline_dir = BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval
                kline_dir.mkdir(parents=True, exist_ok=True)
                output_file = kline_dir / filename
                df.write_parquet(output_file)

    logger.debug(f"{trade_type.value} {time_interval} API klines download successfully")


def _get_missing_kline_dates_for_symbol(
    trade_type: TradeType,
    symbol: str,
    time_interval: str,
    overwrite: bool,
) -> list[str]:
    """Get missing kline dates for a single symbol.
    
    Args:
        trade_type: Type of trade (spot or futures)
        symbol: Trading symbol
        time_interval: Time interval for klines
        overwrite: Whether to overwrite existing files
        
    Returns:
        List of dates with missing kline data
    """
    parsed_kline_dir = BINANCE_DATA_DIR / "parsed_data" / trade_type.value / "klines"
    parsed_symbol_kline_dir = parsed_kline_dir / symbol / time_interval
    ts_mgr = TSManager(parsed_symbol_kline_dir)
    df_cnt = ts_mgr.get_row_count_per_date(exclude_empty=False)
    expected_num = timedelta(days=1) // convert_interval_to_timedelta(time_interval)

    if df_cnt is None:
        return []

    df_missing = df_cnt.filter(pl.col("row_count") < expected_num)
    dts = set(df_missing["dt"])

    if not overwrite:
        api_kline_dir = BINANCE_DATA_DIR / "api_data" / trade_type.value / "klines" / symbol / time_interval
        kline_files = api_kline_dir.glob("*.pqt")
        dts_exist = {convert_date(f.stem) for f in kline_files}
        dts -= dts_exist

    return sorted(dts)


async def download_missing_kline_symbols(
    trade_type: TradeType,
    symbols: list[str],
    time_interval: str,
    overwrite: bool,
    http_proxy: Optional[str],
):
    """Download missing kline data for multiple symbols.
    
    Args:
        trade_type: Type of trade (spot or futures)
        symbols: List of trading symbols
        time_interval: Time interval for klines
        overwrite: Whether to overwrite existing files
        http_proxy: HTTP proxy to use
        
    """
    sym_dts = []
    now = datetime.now()
    with tqdm(total=len(symbols), ncols=100, desc=f"\033[92m{now.strftime('%H:%M:%S')}\033[0m | Missing Klines |", colour="green") as pbar:
        for symbol in symbols:
            missing_dates = _get_missing_kline_dates_for_symbol(trade_type, symbol, time_interval, overwrite)
            sym_dts.extend((symbol, dt) for dt in missing_dates)
            pbar.update(1) 

    if sym_dts:
        await api_download_kline(trade_type, time_interval, sym_dts, http_proxy)


async def download_missing_kline_type(
    trade_type: TradeType,
    time_interval: str,
    overwrite: bool,
    http_proxy: Optional[str],
):
    logger.info(f"BHDS Download missing {trade_type.value} {time_interval} klines from API")

    symbols = local_list_kline_symbols(trade_type, time_interval)
    await download_missing_kline_symbols(trade_type, symbols, time_interval, overwrite, http_proxy)
    
    logger.debug("All missings downloaded")
