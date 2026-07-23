import multiprocessing as mp
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

import polars as pl
from tqdm import tqdm

from aws.download.util import local_list_kline_symbols
from config import (
    BINANCE_DATA_DIR,
    N_JOBS,
    TradeType,
)
from util.concurrent import mp_env_init
from util.log_kit import logger
from util.time import convert_interval_to_timedelta
from util.ts_manager import TSManager

ONLY_SWAP = ['LABUSDT','UBUSDT','OUSDT', 'TERUSDT', 'EWTUSDT','4USDT','AIAUSDT','AKEUSDT','BUSDT','HUSDT','INUSDT','MUSDT','OLUSDT','ONUSDT','QUSDT','TAUSDT','1000XUSDT','CCUSDT', 'IRUSDT', 'USUSDT', 'SP0_AIAUSDT', 'MSTRUSDT', 'IPUSDT', 'ARMUSDT','BEUSDT','VUSDT']


def scan_gaps(df: pl.DataFrame, min_days: int, min_price_chg: float) -> pl.DataFrame:
    '''Scan for gaps where time gap > min_days and abs price change > min_price_chg'''
    ldf = df.lazy()
    ldf = ldf.with_columns(
        pl.col('candle_begin_time').diff().alias('time_diff'),
        (pl.col('open') / pl.col('close').shift() - 1).alias('price_change'),
        pl.col('candle_begin_time').shift().alias('prev_begin_time'),
        pl.col('close').shift().alias('prev_close'),
    )
    min_delta = timedelta(days=min_days)
    df_gap = ldf.filter((pl.col('time_diff') > min_delta) & (pl.col('price_change').abs() > min_price_chg))
    df_gap = df_gap.select('prev_begin_time', 'candle_begin_time', 'prev_close', 'open', 'time_diff', 'price_change')
    return df_gap.collect()


def split_by_gaps(df: pl.DataFrame, df_gap: pl.DataFrame, symbol: str) -> dict[str, pl.DataFrame]:
    '''Split DataFrame into segments at gap boundaries; last segment keeps original symbol name'''
    if df_gap.is_empty():
        return {symbol: df}

    gap_times = df_gap.get_column('candle_begin_time').to_list()
    dfs = []
    for i, gap_time in enumerate(gap_times):
        if i == 0:
            split_df = df.filter(pl.col('candle_begin_time') < gap_time)
        else:
            prev_gap_time = gap_times[i - 1]
            split_df = df.filter(pl.col('candle_begin_time').is_between(prev_gap_time, gap_time, closed='left'))
        if not split_df.is_empty():
            dfs.append(split_df)

    final_df = df.filter(pl.col('candle_begin_time') >= gap_times[-1])
    if not final_df.is_empty():
        dfs.append(final_df)

    return {(f'SP{i}_{symbol}' if i < len(dfs) - 1 else symbol): df for i, df in enumerate(dfs)}


def fill_kline_gaps(df: pl.DataFrame, time_interval: str) -> pl.DataFrame:
    '''Fill missing kline rows with previous close and zero volume'''
    if df.is_empty():
        return df

    complete_times = pl.datetime_range(
        df['candle_begin_time'].min(),
        df['candle_begin_time'].max(),
        interval=convert_interval_to_timedelta(time_interval),
        time_zone='UTC',
        time_unit=df['candle_begin_time'].dtype.time_unit,
        eager=True,
    )  # type: ignore

    ldf = pl.LazyFrame({'candle_begin_time': complete_times}).join(df.lazy(), on='candle_begin_time', how='left')
    ldf = ldf.with_columns(pl.col('close').fill_null(strategy='forward'))
    ldf = ldf.with_columns(
        pl.col('open').fill_null(pl.col('close')),
        pl.col('high').fill_null(pl.col('close')),
        pl.col('low').fill_null(pl.col('close')),
    )

    if 'avg_price_1m' in df.columns:
        ldf = ldf.with_columns(pl.col('avg_price_1m').fill_null(pl.col('open')))
        ldf = ldf.with_columns(pl.col('avg_price_1m').clip(pl.col('low'), pl.col('high')))
        ldf = ldf.with_columns(pl.col('avg_price_1m').fill_nan(pl.col('open')))

    if 'funding_rate' in df.columns:
        ldf = ldf.with_columns(pl.col('funding_rate').fill_null(0))

    ldf = ldf.with_columns(
        pl.col('trade_num').fill_null(0),
        pl.col('taker_buy_base_asset_volume').fill_null(0),
        pl.col('taker_buy_quote_asset_volume').fill_null(0),
        pl.col('volume').fill_null(0),
        pl.col('quote_volume').fill_null(0),
    )
    return ldf.collect()


def merge_klines(trade_type: TradeType, symbol: str, time_interval: str, exclude_empty: bool) -> pl.DataFrame:
    '''Merge AWS parsed klines with API klines; API rows win on duplicate timestamps'''
    parsed_symbol_kline_dir = BINANCE_DATA_DIR / 'parsed_data' / trade_type.value / 'klines' / symbol / time_interval
    aws_df = TSManager(parsed_symbol_kline_dir).read_all()
    if aws_df is None or aws_df.is_empty():
        return pl.DataFrame()

    if exclude_empty:
        aws_df = aws_df.filter(pl.col('volume') > 0)

    api_kline_dir = BINANCE_DATA_DIR / 'api_data' / trade_type.value / 'klines' / symbol / time_interval
    api_files = list(api_kline_dir.glob('*.pqt'))
    if not api_files:
        return aws_df

    api_df = pl.read_parquet(api_files, columns=aws_df.columns)
    if exclude_empty:
        api_df = api_df.filter(pl.col('volume') > 0)

    return pl.concat([aws_df, api_df]).unique(subset=['candle_begin_time'], keep='last').sort('candle_begin_time')


def merge_funding_rates(trade_type: TradeType, symbol: str) -> pl.DataFrame:
    '''Merge AWS parsed funding rates with API funding rates; API rows win on duplicates'''
    cols = ['candle_begin_time', 'funding_rate']
    parsed_funding_file = BINANCE_DATA_DIR / 'parsed_data' / trade_type.value / 'funding' / f'{symbol}.pqt'
    api_funding_file = BINANCE_DATA_DIR / 'api_data' / trade_type.value / 'funding_rate' / f'{symbol}.pqt'

    aws_df = pl.read_parquet(parsed_funding_file, columns=cols) if parsed_funding_file.exists() else pl.DataFrame()
    api_df = pl.read_parquet(api_funding_file, columns=cols) if api_funding_file.exists() else pl.DataFrame()

    dfs = [df for df in (aws_df, api_df) if not df.is_empty()]
    if not dfs:
        return pl.DataFrame()
    merged_df = pl.concat(dfs) if len(dfs) > 1 else dfs[0]
    return merged_df.unique(subset=['candle_begin_time'], keep='last').sort('candle_begin_time')


def gen_kline(trade_type: TradeType, time_interval: str, symbol: str, results_dir: Path, split_gaps: bool, min_days: int, min_price_chg: float, with_vwap: bool, with_funding_rates: bool):
    '''Merge AWS+API klines for one symbol, optionally attach funding/spot_exist, split by gaps, fill and write'''
    # ================================================
    # 1. 合并 K 线
    # ================================================
    df = merge_klines(trade_type, symbol, time_interval, True)
    if df.is_empty():
        return symbol

    if with_vwap:
        df = df.with_columns((pl.col('quote_volume') / pl.col('volume')).alias(f'avg_price_{time_interval}'))

    # ================================================
    # 2. 附加 funding_rate + spot_exist (仅合约)
    # ================================================
    if trade_type in (TradeType.um_futures, TradeType.cm_futures) and with_funding_rates:
        # 2.1. 合并 funding_rate
        df_funding = merge_funding_rates(trade_type, symbol)
        if not df_funding.is_empty():
            df = df.join(df_funding, on='candle_begin_time', how='left').fill_null(0)
        else:
            df = df.with_columns(pl.lit(0).alias('funding_rate'))

        # 2.2. 附加 spot_exist
        spot_dir = BINANCE_DATA_DIR / 'results_data' / 'spot' / '1m'
        base_sym = symbol.replace('SP0_', '')
        spot_files = list(spot_dir.glob(f'{base_sym}.pqt')) + list(spot_dir.glob(f'{symbol.replace("1000", "")}.pqt')) + list(spot_dir.glob(f'1000{symbol}.pqt'))
        if spot_files:
            if symbol in ONLY_SWAP:
                logger.warning(f'Spot data found for {symbol} in ONLY_SWAP')
            spot_time_range = pl.scan_parquet(spot_files[0]).select(
                pl.col('candle_begin_time').min().alias('min_time'),
                pl.col('candle_begin_time').max().alias('max_time'),
            ).collect()
            df = df.with_columns(
                pl.col('candle_begin_time').is_between(spot_time_range.item(0, 'min_time'), spot_time_range.item(0, 'max_time'), closed='both').alias('spot_exist')
            ).fill_null(False)
        else:
            similar_files = list(spot_dir.glob(f'*{base_sym.replace("1000", "")}.pqt'))
            if similar_files and symbol not in ONLY_SWAP:
                logger.warning(f'Spot data not found for {symbol}, found similar: {[i.stem for i in similar_files]}')
            df = df.with_columns(pl.lit(False).alias('spot_exist'))

    # ================================================
    # 3. 按 gap 切分并填补后写出
    # ================================================
    splited_dfs = {symbol: df}
    if split_gaps:
        df_gap = pl.concat([scan_gaps(df, min_days, min_price_chg), scan_gaps(df, min_days * 2, 0)]).unique('candle_begin_time', keep='last')
        splited_dfs = split_by_gaps(df, df_gap, symbol)

    for symbol, df in splited_dfs.items():
        df = fill_kline_gaps(df, time_interval)
        df = df.with_columns(pl.lit(symbol).alias('symbol'))
        df.write_parquet(results_dir / f'{symbol}.pqt')

    return symbol


def gen_kline_type(trade_type: TradeType, time_interval: str, split_gaps: bool, min_days: int, min_price_chg: float, with_vwap: bool, with_funding_rates: bool):
    logger.info(f'BHDS Merge klines for {trade_type.value} {time_interval}')

    results_dir = BINANCE_DATA_DIR / 'results_data' / trade_type.value / time_interval
    symbols = local_list_kline_symbols(trade_type, time_interval)

    if results_dir.exists():
        logger.debug(f'results_dir exists, removing {results_dir}')
        shutil.rmtree(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    run_func = partial(
        gen_kline,
        trade_type=trade_type,
        time_interval=time_interval,
        results_dir=results_dir,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
        with_vwap=with_vwap,
        with_funding_rates=with_funding_rates,
    )

    with ProcessPoolExecutor(max_workers=N_JOBS, mp_context=mp.get_context('spawn'), initializer=mp_env_init) as exe:
        tasks = [exe.submit(run_func, symbol=symbol) for symbol in symbols]
        with tqdm(total=len(tasks), ncols=100, desc=f'\033[92m{datetime.now().strftime("%H:%M:%S")}\033[0m | Merge |', colour='green') as pbar:
            for task in as_completed(tasks):
                symbol = task.result()
                pbar.set_postfix_str(symbol)
                pbar.update(1)
