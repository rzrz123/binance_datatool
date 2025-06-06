import time
import asyncio
from tqdm import tqdm
import pandas as pd
from util.log_kit import logger

from config import BYBIT_DATA_DIR
from api.binance import BinanceBaseApi, BinanceRequestException
from util.network import create_aiohttp_session


def get_interval_stamps(interval_ms: int, start_timestamp_ms: int, end_timestamp_ms: int=0) -> list:
    # 将间隔分钟转换为毫秒 (1000 分钟 = 1000 * 60 * 1000 毫秒)
    # interval_ms = 999 * 60 * 1000
    # 将间隔分钟转换为毫秒 (1小时 = 1000 * 60 * 60 * 1000 毫秒)
    # interval_ms = 60 * 60 * 1000

    # 获取当前时间戳（毫秒）
    if end_timestamp_ms == 0:
        end_timestamp_ms = int(time.time() * 1000)

    # 计算所有间隔的起始时间戳
    timestamps = []
    current = start_timestamp_ms
    while current <= end_timestamp_ms:
        timestamps.append(current)
        current += interval_ms
    timestamps.append(end_timestamp_ms)

    return timestamps


class BybitLinearApi(BinanceBaseApi):
    PREFIX = 'https://api.bybit.com'
    counter = 0
    lock = asyncio.Lock()  # 添加异步锁

    async def aioreq_klines(self, **kwargs) -> list:
        """
        Get Kline/candlestick bars for a symbol.
        Klines are uniquely identified by their open time.
        """
        async with self.lock:  # 使用锁保护计数器
            self.counter += 1
            if self.counter >= 550:
                self.counter = 0
                logger.debug("Bybit Linear API rate limit exceeded, sleeping for 5 seconds")
                await asyncio.sleep(5)

        url = f'{self.PREFIX}/v5/market/kline'
        retrys = 0
        while True:
            try:
                return await self._aio_get(url, kwargs)
            except BinanceRequestException as e:
                retrys += 1
                if retrys > 3:
                    raise e
                await asyncio.sleep(retrys)


    async def aioreq_funding_rate(self, **kwargs):
        async with self.lock:  # 使用锁保护计数器
            self.counter += 1
            if self.counter >= 550:
                self.counter = 0
                logger.debug("Bybit Linear API rate limit exceeded, sleeping for 5 seconds")
                await asyncio.sleep(5)

        url = f'{self.PREFIX}/v5/market/funding/history'
        return await self._aio_get(url, kwargs)


    async def aioreq_instruments_info(self, **kwargs):
        url = f'{self.PREFIX}/v5/market/instruments-info'
        return (await self._aio_get(url, kwargs))['result']['list']


async def get_bybit_linear_instruments_info() -> list[dict]:
    logger.info("Getting Bybit linear instruments info")
    async with create_aiohttp_session(15) as session:
        res = await BybitLinearApi(session, '').aioreq_instruments_info(category='linear')
        return res['result']['list']


async def download_bybit_linear_klines() -> None:
    logger.info("Getting Bybit linear klines")
    async with create_aiohttp_session(timeout_sec=1200) as session:
        api = BybitLinearApi(session, '')
        ins_info = await api.aioreq_instruments_info(category='linear')
        ins_info = [i for i in ins_info if i['contractType'] == 'LinearPerpetual']
        pbar = tqdm(total=len(ins_info), ncols=100, colour="green")
        tasks = [asyncio.create_task(download_bybit_linear_klines_symbol(api, symbol_info, pbar)) for symbol_info in ins_info]
        await asyncio.gather(*tasks)


async def download_bybit_linear_klines_symbol(api: BybitLinearApi, symbol_info: dict, pbar: tqdm) -> None:

    # 0. read parquet if exists
    filename =  f"{symbol_info['symbol']}.pqt"
    kline_dir = BYBIT_DATA_DIR / "linear" / "klines" / symbol_info['symbol'] / "1m"
    kline_dir.mkdir(parents=True, exist_ok=True)
    if (kline_dir / filename).exists():
        existing_df = pd.read_parquet(kline_dir / filename).reset_index()
        last_timestamp = existing_df['candle_begin_time'].max().value//1000000
        if last_timestamp > int(time.time() * 1000) - 4 * 60 * 60 * 1000: # skip 8 hour
            pbar.set_postfix_str(symbol_info['symbol'])
            pbar.update(1)
            return
        symbol_info['launchTime'] = last_timestamp
    else:
        existing_df = pd.DataFrame()

    # 1. create all params
    params_list = [{
        'category': 'linear',
        'symbol': symbol_info['symbol'],
        'interval': 1,
        'start': i,
        'limit': 1000,
    } for i in  get_interval_stamps(60 * 999 * 1000, int(symbol_info['launchTime']), int(symbol_info['deliveryTime']))]

    # 2. create all tasks
    tasks = [asyncio.create_task(api.aioreq_klines(**params)) for params in params_list]

    # 3. get all klines
    res = await asyncio.gather(*tasks)
    klines = []
    for i in res:
        klines.extend(i['result']['list'])
    # 4. transform to df
    df = pd.DataFrame(klines, columns=[
        'candle_begin_time',  # K线开始时间 'startTime'
        'open',  # 开盘价 'openPrice'
        'high',  # 最高价 'highPrice'
        'low',  # 最低价 'lowPrice'
        'close',  # 收盘价 'closePrice'
        'volume',  # 成交量 'volume'
        'quote_volume'  # 成交额(报价货币的数量，通常是USDT, 少部分为BTC等) 'turnover'
    ])
    df = df.astype({
        'candle_begin_time': 'int64',
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'float64', 
        'quote_volume': 'float64',
    })
    df['candle_begin_time'] = pd.to_datetime(pd.to_numeric(df['candle_begin_time']), unit='ms').dt.tz_localize('UTC')
    # 5. save to parquet
    (
        pd.concat([existing_df,df],ignore_index=True)
        .drop_duplicates(subset=['candle_begin_time'], keep='last')
        .sort_values(by='candle_begin_time')
        .set_index('candle_begin_time')
        .to_parquet(kline_dir / filename)
    )
    pbar.set_postfix_str(symbol_info['symbol'])
    pbar.update(1)


async def download_bybit_linear_funding_rates() -> None:
    logger.info("Getting Bybit linear funding rates")
    async with create_aiohttp_session(timeout_sec=60) as session:
        api = BybitLinearApi(session, '')
        ins_info = await api.aioreq_instruments_info(category='linear')
        ins_info = [i for i in ins_info if i['contractType'] == 'LinearPerpetual']
        pbar = tqdm(total=len(ins_info), ncols=100, colour="green")
        tasks = [asyncio.create_task(download_bybit_linear_funding_rates_symbol(api, symbol_info, pbar)) for symbol_info in ins_info]
        await asyncio.gather(*tasks)


async def download_bybit_linear_funding_rates_symbol(api: BybitLinearApi, symbol_info: dict, pbar: tqdm) -> None:
    # 0. read parquet if exists
    filename =  f"{symbol_info['symbol']}.pqt"
    dir_funding = BYBIT_DATA_DIR / "linear" / "funding"
    dir_funding.mkdir(parents=True, exist_ok=True)
    if (dir_funding / filename).exists():
        existing_df = pd.read_parquet(dir_funding / filename).reset_index()
        last_timestamp = existing_df['candle_begin_time'].max().value//1000000
        symbol_info['launchTime'] = last_timestamp
    else:
        existing_df = pd.DataFrame()

    # 1. create all params
    params_list = [{
        'category': 'linear',
        'symbol': symbol_info['symbol'],
        'startTime': int(symbol_info['launchTime']),
        'endTime': int(time.time() * 1000),
        'limit': 200,
    }]

    # 2. create all tasks
    funding_rates = []
    while True:
        res = await api.aioreq_funding_rate(**params_list[0])
        if len(res['result']['list']) <= 1:
            break
        funding_rates.extend(res['result']['list'])
        params_list[0]['endTime'] = res['result']['list'][-1]['fundingRateTimestamp']
    if funding_rates == []:
        pbar.set_postfix_str(symbol_info['symbol'])
        pbar.update(1)
        return
    # 4. transform to df
    df = pd.DataFrame(funding_rates).drop(columns=['symbol'])
    df.rename(columns={'fundingRateTimestamp': 'candle_begin_time', 'fundingRate': 'funding_rate'}, inplace=True)
    df = df.astype({
        'candle_begin_time': 'int64',
        'funding_rate': 'float64',
    })
    df['candle_begin_time'] = pd.to_datetime(pd.to_numeric(df['candle_begin_time']), unit='ms').dt.tz_localize('UTC')
    # 5. save to parquet
    (
        pd.concat([existing_df,df],ignore_index=True)
        .drop_duplicates(subset=['candle_begin_time'], keep='last')
        .sort_values(by='candle_begin_time')
        .set_index('candle_begin_time')
        .to_parquet(dir_funding / filename)
    )
    pbar.set_postfix_str(symbol_info['symbol'])
    pbar.update(1)

if __name__ == "__main__":
    asyncio.run(download_bybit_linear_funding_rates())