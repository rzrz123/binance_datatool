import os
import time
import asyncio
from datetime import datetime
import numpy as np
from tqdm import tqdm
import pandas as pd
from util.log_kit import logger

from config import config
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
            if self.counter >= 600:
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
            if self.counter >= 600:
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
    async with create_aiohttp_session(timeout_sec=60) as session:
        api = BybitLinearApi(session, '')
        ins_info = await api.aioreq_instruments_info(category='linear')
        pbar = tqdm(total=len(ins_info), ncols=100, colour="green")
        tasks = [asyncio.create_task(download_bybit_linear_klines_symbol(api, symbol_info, pbar)) for symbol_info in ins_info]
        await asyncio.gather(*tasks)


async def download_bybit_linear_klines_symbol(api: BybitLinearApi, symbol_info: dict, pbar: tqdm) -> None:
    pbar.set_postfix_str(symbol_info['symbol'])
    pbar.update(1)
    # 0. read parquet if exists
    filename =  f"{symbol_info['symbol']}.pqt"
    kline_dir = config.BYBIT_DATA_DIR / "linear" / "klines" / symbol_info['symbol'] / "1m"
    kline_dir.mkdir(parents=True, exist_ok=True)
    if (kline_dir / filename).exists():
        existing_df = pd.read_parquet(kline_dir / filename)
        last_timestamp = existing_df['candle_begin_time'].max().value//1000000
        if last_timestamp > int(time.time() * 1000) - 4 * 60 * 60 * 1000: # skip 8 hour
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
    df['candle_begin_time'] = pd.to_datetime(pd.to_numeric(df['candle_begin_time']), unit='ms')
    df['symbol'] = symbol_info['symbol']
    df['category'] = symbol_info['contractType']
    # 5. save to parquet
    (
        pd.concat([existing_df,df],ignore_index=True)
        .drop_duplicates(subset=['candle_begin_time'], keep='last')
        .sort_values(by='candle_begin_time')
        .to_parquet(kline_dir / filename)
    )


async def download_bybit_linear_funding_rates() -> None:
    logger.info("Getting Bybit linear funding rates")
    async with create_aiohttp_session(timeout_sec=60) as session:
        api = BybitLinearApi(session, '')
        ins_info = await api.aioreq_instruments_info(category='linear')
        pbar = tqdm(total=len(ins_info), ncols=100, colour="green")
        tasks = [asyncio.create_task(download_bybit_linear_funding_rates_symbol(api, symbol_info, pbar)) for symbol_info in ins_info]
        await asyncio.gather(*tasks)


async def download_bybit_linear_funding_rates_symbol(api: BybitLinearApi, symbol_info: dict, pbar: tqdm) -> None:
    pbar.set_postfix_str(symbol_info['symbol'])
    pbar.update(1)

    # 0. read parquet if exists
    filename =  f"{symbol_info['symbol']}.pqt"
    dir_funding = config.BYBIT_DATA_DIR / "linear" / "funding"
    dir_funding.mkdir(parents=True, exist_ok=True)
    if (dir_funding / filename).exists():
        existing_df = pd.read_parquet(dir_funding / filename)
        last_timestamp = existing_df['fundingRateTimestamp'].max().value//1000000
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
        return
    # 4. transform to df
    df = pd.DataFrame(funding_rates)
    df['fundingRateTimestamp'] = pd.to_datetime(pd.to_numeric(df['fundingRateTimestamp']), unit='ms')
    # 5. save to parquet
    (
        pd.concat([existing_df,df],ignore_index=True)
        .drop_duplicates(subset=['fundingRateTimestamp'], keep='last')
        .sort_values(by='fundingRateTimestamp')
        .to_parquet(dir_funding / filename)
    )


def save_linear_candle_data(symbol, start_time, now_time, path):
    print(f'正在获取linear-{symbol}数据')

    # 判断路径是否存在
    if os.path.exists(path) is False:
        os.mkdir(path)

    # 判断bybit路径是否存在
    path = os.path.join(path, 'bybit')
    if os.path.exists(path) is False:
        os.mkdir(path)

    # 判断spot路径是否存在
    path = os.path.join(path, 'linear')
    if os.path.exists(path) is False:
        os.mkdir(path)

    # 判断1h路径是否存在
    path = os.path.join(path, '1h')
    if os.path.exists(path) is False:
        os.mkdir(path)

    # 保存文件
    file_name = symbol.split('USDT')[0] + '-USDT' + '.csv'
    path = os.path.join(path, file_name)

    # 判断是否为第一次获取
    if os.path.exists(path) is False:
        # 多一列资金费率数据
        df = pd.DataFrame(
            columns=['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume',
                     'Spread', 'symbol', 'avg_price_1m', 'avg_price_5m', 'fundingRate'])
    else:
        df = pd.read_csv(path)
        if df.empty:
            pass
        else:
            start_time = df['candle_begin_time'].iloc[-1]
            start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            start_time = int(start_time.timestamp() * 1000)

    linear_list_all = []
    funding_list_all = []


    # 当前时间作为end_time
    end_time = now_time

    while True:
        params_linear_funding = {
            'category': 'linear',
            'symbol': symbol,
            'endTime': end_time,
            'limit': 1000,
        }

        funding_list = exchange.publicGetV5MarketFundingHistory(params=params_linear_funding)['result']['list']
        # print(funding_list)
        funding_list_all += funding_list

        # 如果获取不到新的数据，则跳出循环
        if len(funding_list) == 1:
            break

        end_time = int(funding_list[-1]['fundingRateTimestamp'])

    df_funding = pd.DataFrame(funding_list_all)
    # print(df_funding)

    # 时间戳转化、截取数据、去重、排序、重置索引
    df_funding['candle_begin_time'] = pd.to_datetime(df_funding['fundingRateTimestamp'], unit='ms')
    df_funding = df_funding[['candle_begin_time', 'fundingRate']]
    df_funding.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df_funding.sort_values(by='candle_begin_time', inplace=True)
    df_funding.reset_index(drop=True, inplace=True)
    # print(df_funding)
    # exit()

    while True:
        params_linear = {
            'category': 'linear',
            'symbol': symbol,
            'interval': 1,  # 1,3,5,15,30,60,120,240,360,720,D,M,W
            'start': start_time,
            'limit': 1000,
        }

        linear_list = exchange.publicGetV5MarketKline(params=params_linear)['result']['list']
        linear_list_all += linear_list

        # 如果获取为空，增加1天的时间量
        if linear_list == []:
            start_time += 86400000
            continue

        # 如果获取不到数据，则跳出循环
        if start_time >= now_time:
            break

        # print(start_time)
        start_time = int(linear_list[0][0])

    df_linear_1m = pd.DataFrame(linear_list_all, dtype=float)
    # print(df_linear_1m)
    # exit()

    # 重命名
    df_linear_1m.rename(columns={
        0: 'candle_begin_time',  # K线开始时间 'startTime'
        1: 'open',  # 开盘价 'openPrice'
        2: 'high',  # 最高价 'highPrice'
        3: 'low',  # 最低价 'lowPrice'
        4: 'close',  # 收盘价 'closePrice'
        5: 'volume',  # 成交量 'volume'
        6: 'quote_volume'  # 成交额(报价货币的数量，通常是USDT, 少部分为BTC等) 'turnover'
    }, inplace=True)

    # 将时间戳转成时间 1.713866e+12 → 2024-04-23 10:00:00
    df_linear_1m['candle_begin_time'] = pd.to_datetime(df_linear_1m['candle_begin_time'], unit='ms')
    # 新增symbol列
    if symbol.endswith('USDT'):
        df_linear_1m['symbol'] = symbol.split('USDT')[0] + '-USDT'
    else:
        df_linear_1m['symbol'] = symbol

    df_linear_1m.set_index('candle_begin_time', inplace=True)
    df_linear_1m['avg_price_1m'] = df_linear_1m['quote_volume'] / df_linear_1m['volume']
    df_linear_1m['avg_price_5m'] = df_linear_1m.resample(rule='5T')['avg_price_1m'].mean()
    df_linear_1m['avg_price_1m'].fillna(value=df_linear_1m['open'], inplace=True)  # 没有1分钟均价就使用开盘价
    df_linear_1m['avg_price_5m'].fillna(value=df_linear_1m['avg_price_1m'], inplace=True)  # 没有5分钟均价就使用1分钟均价
    df_linear_1m['Spread'] = np.nan

    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'Spread': 'last',
        'symbol': 'first',
        'avg_price_1m': 'first',
        'avg_price_5m': 'first',
    }

    df_merged_1h = df_linear_1m.resample(rule='1H').agg(agg_dict)
    df_merged_1h = df_merged_1h.reset_index()

    # df与df_merged_1h合并
    df_merge = pd.concat([df, df_merged_1h], ignore_index=True)

    # # df_funding与df_merged合并
    df_merge['candle_begin_time'] = pd.to_datetime(df_merge['candle_begin_time'])
    df_merge_temp = pd.merge(df_merge, df_funding, on='candle_begin_time', suffixes=('_df_merge', '_df_funding'),how='left')
    df_merge['fundingRate'] = df_merge_temp['fundingRate_df_funding'].fillna(df_merge_temp['fundingRate_df_merge'])

    # 数据填充
    df_merge['symbol'].fillna(method='ffill', inplace=True)
    df_merge['close'] = df_merge['close'].fillna(method='ffill')
    df_merge['open'] = df_merge['open'].fillna(df['close'])
    df_merge['high'] = df_merge['high'].fillna(df['close'])
    df_merge['low'] = df_merge['low'].fillna(df['close'])
    df_merge['volume'] = df_merge['volume'].fillna(0)
    df_merge['quote_volume'] = df_merge['quote_volume'].fillna(0)

    # 列重新排序
    df_merge = df_merge[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume',
                            'Spread', 'symbol', 'avg_price_1m', 'avg_price_5m', 'fundingRate']]
    # 去重
    df_merge['candle_begin_time'] = pd.to_datetime(df_merge['candle_begin_time'])
    df_merge.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    # 排序
    df_merge.sort_values(by='candle_begin_time', inplace=True)
    # 重置索引
    df_merge.reset_index(drop=True, inplace=True)

    # 保存文件
    # df_merge.to_csv(path, index=False, mode='w', encoding='gbk')


if __name__ == '__main__':
    '''
    USTC在2022-5-12后失去稳定币的性质，需特别处理。
    '''

    # 1. instruments_info
    # ins_info = asyncio.run(get_bybit_linear_instruments_info())

    # ================================
    # 2. klines
    asyncio.run(download_bybit_linear_klines())
    asyncio.run(download_bybit_linear_funding_rates())
