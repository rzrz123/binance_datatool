import asyncio
import json

import aiohttp
import pandas as pd
import polars as pl

from aws.download.util import local_list_kline_symbols
from config import BINANCE_DATA_DIR, HTTP_TIMEOUT_SEC, TradeType
from util.log_kit import logger
from util.network import async_retry_getter, create_aiohttp_session


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


class BinanceFuturesMarketApi(BinanceBaseApi):
    '''USD-M / COIN-M market endpoints used to fill the open funding month.'''

    PREFIX: str
    API_VER = 'v1'

    async def aioreq_funding_rate(self, **kwargs):
        url = f'{self.PREFIX}/{self.API_VER}/fundingRate'
        return await self._aio_get(url, kwargs)


class BinanceMarketUMFapi(BinanceFuturesMarketApi):
    '''Binance USD-M Futures Fapi market endpoints'''

    PREFIX = 'https://fapi.binance.com/fapi'

    async def aioreq_exchange_info(self) -> dict:
        url = f'{self.PREFIX}/{self.API_VER}/exchangeInfo'
        return await self._aio_get(url, None)

    async def aioreq_list_tradifi_symbols(self) -> list:
        exchange_info = await self.aioreq_exchange_info()
        symbols_info = pd.DataFrame(exchange_info['symbols'])  # type: ignore
        symbols_info['deliveryDate'] = pd.to_datetime(pd.to_numeric(symbols_info['deliveryDate']), unit='ms')  # type: ignore
        symbols_info['baseAsset1k'] = symbols_info['baseAsset'].str.replace('1000', '')
        symbols_info['baseAsset1k'] = symbols_info['baseAsset1k'].str.replace('SATS', '1000SATS')
        symbols_info = symbols_info.query("contractType == 'TRADIFI_PERPETUAL'")
        return symbols_info['symbol'].tolist()


class BinanceMarketCMDapi(BinanceFuturesMarketApi):
    '''Binance COIN-M Futures Dapi market endpoints'''

    PREFIX = 'https://dapi.binance.com/dapi'


def create_binance_market_api(trade_type: TradeType, session) -> BinanceFuturesMarketApi:
    match trade_type:
        case TradeType.um_futures:
            return BinanceMarketUMFapi(session)
        case TradeType.cm_futures:
            return BinanceMarketCMDapi(session)
        case _:
            raise RuntimeError(f'Cannot request funding rate for {trade_type.value}')


class BinanceFetcher:

    def __init__(self, trade_type: TradeType, session: aiohttp.ClientSession):
        self.trade_type = trade_type
        self.market_api = create_binance_market_api(trade_type, session)

    async def get_hist_funding_rate(self, symbol, **kwargs):
        '''Parse historical funding rates from /fundingRate. AWS files are monthly, so this covers the open month.'''
        # Wait for 75s after a failure because the rate limit is 500/5mins
        data = await async_retry_getter(self.market_api.aioreq_funding_rate, symbol=symbol, _sleep_seconds=75, **kwargs)
        if not data:
            return None

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
            if df_funding is None or df_funding.is_empty():
                continue
            df_funding.write_parquet(funding_dir / f'{symbol}.pqt')

    logger.debug(f'{trade_type.value} {symbols[0]} -- {symbols[-1]} API Funding Rates download successfully')


async def download_funding_rates_all(trade_type: TradeType):
    '''AWS fundingRate is monthly, so the open month has to be filled from the REST API.'''
    logger.info(f'BHDS Recent {trade_type.value} Funding Rates API Download')
    symbols = local_list_kline_symbols(trade_type, '1m')
    await download_funding_rates(trade_type, symbols)
