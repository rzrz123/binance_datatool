import asyncio
import json
from datetime import datetime, timedelta
from datetime import time as dtime
from decimal import Decimal
from zoneinfo import ZoneInfo

import aiohttp
import pandas as pd
import polars as pl
from dateutil import parser as date_parser
from tqdm import tqdm

from aws.download.util import local_list_kline_symbols
from config import BINANCE_DATA_DIR, HTTP_TIMEOUT_SEC, TradeType
from util.log_kit import logger
from util.network import async_retry_getter, create_aiohttp_session
from util.time import (
    async_sleep_until_run_time,
    convert_date,
    convert_interval_to_timedelta,
    next_run_time,
)
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

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

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
        async with self.session.get(url, params=params) as resp:
            return await self._handle_response(resp)

    async def _aio_post(self, url, params):
        async with self.session.post(url, data=params) as resp:
            return await self._handle_response(resp)


class BinanceBaseMarketApi(BinanceBaseApi):
    WEIGHT_EFFICIENT_ONCE_CANDLES = 499
    MAX_MINUTE_WEIGHT = 2400
    MAX_ONCE_CANDLES = 1000
    PREFIX: str
    API_VER = 'v1'

    async def aioreq_time_and_weight(self) -> tuple[int, int]:
        url = f'{self.PREFIX}/{self.API_VER}/time'
        async with self.session.get(url) as resp:
            weight = int(resp.headers['X-MBX-USED-WEIGHT-1M'])
            timestamp = (await resp.json())['serverTime']
        return timestamp, weight

    async def aioreq_klines(self, **kwargs) -> list:
        url = f'{self.PREFIX}/{self.API_VER}/klines'
        return await self._aio_get(url, kwargs)

    async def aioreq_exchange_info(self) -> dict:
        url = f'{self.PREFIX}/{self.API_VER}/exchangeInfo'
        return await self._aio_get(url, None)

    async def aioreq_premium_index(self, **kwargs) -> list:
        url = f'{self.PREFIX}/{self.API_VER}/premiumIndex'
        return await self._aio_get(url, kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        url = f'{self.PREFIX}/{self.API_VER}/fundingRate'
        return await self._aio_get(url, kwargs)


class BinanceMarketUMFapi(BinanceBaseMarketApi):
    '''Binance USDⓈ-M Futures Fapi market endpoints'''

    PREFIX = 'https://fapi.binance.com/fapi'
    MAX_ONCE_CANDLES = 1500

    async def aioreq_list_tradifi_symbols(self) -> list:
        exchange_info = await self.aioreq_exchange_info()
        symbols_info = pd.DataFrame(exchange_info['symbols'])  # type: ignore
        symbols_info['deliveryDate'] = pd.to_datetime(pd.to_numeric(symbols_info['deliveryDate']), unit='ms')  # type: ignore
        symbols_info['baseAsset1k'] = symbols_info['baseAsset'].str.replace('1000', '')
        symbols_info['baseAsset1k'] = symbols_info['baseAsset1k'].str.replace('SATS', '1000SATS')
        symbols_info = symbols_info.query("contractType == 'TRADIFI_PERPETUAL'")
        return symbols_info['symbol'].tolist()

    async def aioreq_book_ticker(self, **kwargs):
        url = f'{self.PREFIX}/{self.API_VER}/ticker/bookTicker'
        return await self._aio_get(url, kwargs)


class BinanceMarketCMDapi(BinanceBaseMarketApi):
    '''Binance COIN-M Futures Dapi market endpoints'''

    PREFIX = 'https://dapi.binance.com/dapi'
    MAX_ONCE_CANDLES = 1500


class BinanceMarketSpotApi(BinanceBaseMarketApi):
    '''Binance Spot Api market endpoints'''

    PREFIX = 'https://api.binance.com/api'
    API_VER = 'v3'
    WEIGHT_EFFICIENT_ONCE_CANDLES = 1000
    MAX_MINUTE_WEIGHT = 6000

    async def aioreq_premium_index(self, **kwargs) -> list:
        raise NotImplementedError

    async def aioreq_funding_rate(self, **kwargs):
        raise NotImplementedError


def create_binance_market_api(trade_type: TradeType, session) -> BinanceBaseMarketApi:
    match trade_type:
        case TradeType.spot:
            return BinanceMarketSpotApi(session)
        case TradeType.um_futures:
            return BinanceMarketUMFapi(session)
        case TradeType.cm_futures:
            return BinanceMarketCMDapi(session)


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

    def __init__(self, trade_type: TradeType, session: aiohttp.ClientSession):
        self.trade_type = trade_type
        self.market_api = create_binance_market_api(trade_type, session)

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

    async def get_kline_df(self, symbol, interval, **kwargs) -> pl.DataFrame | None:
        '''Request /klines and convert to polars.DataFrame'''
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
        return lf.collect()

    async def get_kline_df_of_day(self, symbol, interval, dt) -> pl.DataFrame | None:
        '''Request /klines for a given date and convert to polars.DataFrame'''
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
        return lf.collect()

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
        '''Parse historical funding rates from /fundingRate'''
        if self.trade_type == TradeType.spot:
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


async def download_funding_rates(trade_type: TradeType, symbols: list[str]):
    logger.debug(f'Start Download {trade_type.value} {symbols[0]} -- {symbols[-1]} Funding Rates from Binance API')

    funding_dir = BINANCE_DATA_DIR / 'api_data' / trade_type.value / 'funding_rate'
    funding_dir.mkdir(parents=True, exist_ok=True)
    logger.debug(f'Funding rate directory: {funding_dir}')

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session)
        dfs = await asyncio.gather(*[fetcher.get_hist_funding_rate(symbol=symbol, limit=1000) for symbol in symbols])
        for symbol, df_funding in zip(symbols, dfs):
            df_funding.write_parquet(funding_dir / f'{symbol}.pqt')

    logger.debug(f'{trade_type.value} {symbols[0]} -- {symbols[-1]} API Funding Rates download successfully')

async def download_funding_rates_all(trade_type: TradeType):
    logger.info(f'BHDS Recent {trade_type.value} Funding Rates API Download')
    symbols = local_list_kline_symbols(trade_type, '1m')
    await download_funding_rates(trade_type, symbols)


async def api_download_kline(trade_type: TradeType, time_interval: str, sym_dts: list[tuple[str, str]]):
    BATCH_SIZE = 40

    logger.debug(f'Start Download {trade_type.value} {time_interval} {len(sym_dts)} Klines from Binance API')
    sym_dts = sorted([(sym, convert_date(dt)) for sym, dt in sym_dts])  # type: ignore

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        fetcher = BinanceFetcher(trade_type, session)
        while sym_dts:
            server_ts, weight = await fetcher.get_time_and_weight()
            batch, sym_dts = sym_dts[:BATCH_SIZE], sym_dts[BATCH_SIZE:]
            logger.debug(f'server_time={server_ts}, weight_used={weight}, start={batch[0]}, end={batch[-1]}')

            max_minute_weight, _ = fetcher.get_api_limits()
            if weight > max_minute_weight - 480:
                logger.info(f'Weight {weight} exceeds the maximum limit, sleep until next minute')
                await async_sleep_until_run_time(next_run_time('1m'))
                continue

            results = await asyncio.gather(*[fetcher.get_kline_df_of_day(sym, time_interval, dt) for sym, dt in batch])
            for (symbol, dt), df in zip(batch, results):
                if df is None:
                    continue
                filename = dt.strftime('%Y%m%d') + '.pqt'  # type: ignore
                kline_dir = BINANCE_DATA_DIR / 'api_data' / trade_type.value / 'klines' / symbol / time_interval
                kline_dir.mkdir(parents=True, exist_ok=True)
                df.write_parquet(kline_dir / filename)

    logger.debug(f'{trade_type.value} {time_interval} API klines download successfully')


async def download_missing_kline_type(trade_type: TradeType, time_interval: str, overwrite: bool):
    logger.info(f'BHDS Download missing {trade_type.value} {time_interval} klines from API')

    symbols = local_list_kline_symbols(trade_type, time_interval)
    expected_num = timedelta(days=1) // convert_interval_to_timedelta(time_interval)
    parsed_kline_dir = BINANCE_DATA_DIR / 'parsed_data' / trade_type.value / 'klines'

    # ================================================
    # 1. 收集各 symbol 缺失日期
    # ================================================
    sym_dts = []
    now = datetime.now()
    with tqdm(total=len(symbols), ncols=100, desc=f'\033[92m{now.strftime("%H:%M:%S")}\033[0m | Missing Klines |', colour='green') as pbar:
        for symbol in symbols:
            ts_mgr = TSManager(parsed_kline_dir / symbol / time_interval)
            df_cnt = ts_mgr.get_row_count_per_date(exclude_empty=False)
            if df_cnt is None:
                pbar.update(1)
                continue

            dts = set(df_cnt.filter(pl.col('row_count') < expected_num)['dt'])
            if not overwrite:
                api_kline_dir = BINANCE_DATA_DIR / 'api_data' / trade_type.value / 'klines' / symbol / time_interval
                dts -= {convert_date(f.stem) for f in api_kline_dir.glob('*.pqt')}
            sym_dts.extend((symbol, dt) for dt in sorted(dts))
            pbar.update(1)

    # ================================================
    # 2. 下载缺失 K 线
    # ================================================
    if sym_dts:
        await api_download_kline(trade_type, time_interval, sym_dts)

    logger.debug('All missings downloaded')
