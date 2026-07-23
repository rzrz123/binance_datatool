import multiprocessing as mp
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from functools import partial

import polars as pl
from tqdm import tqdm

from config import BINANCE_DATA_DIR, N_JOBS, TradeType
from util.concurrent import mp_env_init
from util.log_kit import logger


def resample_kline(trade_type: TradeType, symbol: str, resample_interval: str, base_offset: str) -> str:
    '''Resample a 1m kline to a higher timeframe with offset'''
    results_dir = BINANCE_DATA_DIR / 'results_data' / trade_type.value
    resampled_offset_dir = results_dir / resample_interval / base_offset
    resampled_offset_dir.mkdir(parents=True, exist_ok=True)

    ldf = pl.scan_parquet(results_dir / '1m' / f'{symbol}.pqt')
    columns = ldf.collect_schema().names()

    # ================================================
    # 1. 聚合规则
    # ================================================
    agg = [
        pl.col('symbol').last(),
        pl.col('open').first(),
        pl.col('high').max(),
        pl.col('low').min(),
        pl.col('close').last(),
        pl.col('volume').sum(),
        pl.col('quote_volume').sum(),
        pl.col('trade_num').sum(),
        pl.col('taker_buy_base_asset_volume').sum(),
        pl.col('taker_buy_quote_asset_volume').sum(),
    ]

    if 'spot_exist' in columns:
        agg.append(pl.col('spot_exist').first())

    if 'avg_price_1m' in columns:
        agg.append(pl.col('avg_price_1m').first())

    if 'funding_rate' in columns:
        has_funding_cond = pl.col('funding_rate').abs() > 1e-6
        agg.extend([
            pl.col('funding_rate').filter(has_funding_cond).first().alias('funding_rate'),
            pl.col('open').filter(has_funding_cond).first().alias('funding_price'),
            pl.col('candle_begin_time').filter(has_funding_cond).first().alias('funding_time'),
        ])

    # ================================================
    # 2. 重采样并写出
    # ================================================
    ldf = ldf.group_by_dynamic('candle_begin_time', every=resample_interval, offset=base_offset).agg(agg).fill_null(0)
    ldf.collect().write_parquet(resampled_offset_dir / f'{symbol}.pqt')
    return symbol


def resample_kline_all(trade_type: TradeType, resample_interval: str, base_offset: str):
    '''Resample kline data for all symbols of a given trade type'''
    logger.info(f'Resample kline {trade_type.value} {resample_interval}')

    resampled_dir = BINANCE_DATA_DIR / 'results_data' / trade_type.value / resample_interval / base_offset
    results_dir = BINANCE_DATA_DIR / 'results_data' / trade_type.value / '1m'
    symbols = sorted(p.stem for p in results_dir.glob('*.pqt'))

    if resampled_dir.exists():
        logger.debug(f'Resampled kline directory exists, removing it {resampled_dir}')
        shutil.rmtree(resampled_dir)

    run_func = partial(resample_kline, trade_type=trade_type, resample_interval=resample_interval, base_offset=base_offset)

    with ProcessPoolExecutor(max_workers=N_JOBS, mp_context=mp.get_context('spawn'), initializer=mp_env_init) as exe:
        tasks = [exe.submit(run_func, symbol=symbol) for symbol in symbols]
        now = datetime.now()
        with tqdm(total=len(tasks), ncols=100, desc=f'\033[92m{now.strftime("%H:%M:%S")}\033[0m | Resample |', colour='green') as pbar:
            for task in as_completed(tasks):
                symbol = task.result()
                pbar.set_postfix_str(symbol)
                pbar.update(1)
