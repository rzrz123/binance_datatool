import multiprocessing as mp
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from functools import partial

import polars as pl
from tqdm import tqdm

from config.config import BINANCE_DATA_DIR, N_JOBS, TradeType, BYBIT_DATA_DIR, ExchangeType, OKX_DATA_DIR
from util.concurrent import mp_env_init
from util.log_kit import logger


def resample_kline(exchange: ExchangeType, trade_type: TradeType, symbol: str, resample_interval: str, base_offset: str) -> str:
    """
    Resample a kline DataFrame to a higher time frame with an offset.
    """
    # 1. Get results directory
    if exchange == "bybit":
        results_dir = BYBIT_DATA_DIR / "results_data" / "linear"
    elif exchange == "binance":
        results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value
    elif exchange == "okx":
        results_dir = OKX_DATA_DIR / "results_data" / "swap"
    else:
        raise ValueError(f"Invalid exchange: {exchange}")

    # 2. Create output directory for this offset
    resampled_offset_dir = results_dir / resample_interval / base_offset
    resampled_offset_dir.mkdir(parents=True, exist_ok=True)

    # 3. Resample data
    ldf = pl.scan_parquet(results_dir / "1m" / f"{symbol}.pqt")
    columns = ldf.collect_schema().names()

    # 3.1 Aggregation rules
    agg = [
        pl.col("symbol").last(),  # Symbol of the resampled kline
        pl.col("open").first(),  # Opening price of the resampled kline
        pl.col("high").max(),  # Highest price during the resampled period
        pl.col("low").min(),  # Lowest price during the resampled period
        pl.col("close").last(),  # Closing price of the resampled kline
        pl.col("volume").sum(),  # Total volume during the resampled period
        pl.col("quote_volume").sum(),  # Total quote volume during the resampled period
    ]
    if exchange == "binance":
        agg += [
            pl.col("trade_num").sum(),  # Total number of trades during the resampled period
            pl.col("taker_buy_base_asset_volume").sum(),  # Total taker buy base asset volume during the resampled period
            pl.col("taker_buy_quote_asset_volume").sum(),  # Total taker buy quote asset volume during the resampled period
        ]

    # 3.2 additional columns if exist
    if "spot_exist" in columns:
        agg.append(pl.col("spot_exist").first())

    if "avg_price_1m" in columns:
        agg.append(pl.col("avg_price_1m").first())

    if "funding_rate" in columns:
        has_funding_cond = pl.col("funding_rate").abs() > 1e-6
        agg.extend([
            pl.col("funding_rate").filter(has_funding_cond).first().alias("funding_rate"),
            pl.col("open").filter(has_funding_cond).first().alias("funding_price"),
            pl.col("candle_begin_time").filter(has_funding_cond).first().alias("funding_time")
        ])

    # 3.3 Group the data by the start time of the klines, resampling to the specified interval with the given offset
    ldf = ldf.group_by_dynamic("candle_begin_time", every=resample_interval, offset=base_offset).agg(agg).fill_null(0)

    # 3.4 Write the resampled data to the output directory
    ldf.collect().write_parquet(resampled_offset_dir / f"{symbol}.pqt")

    return symbol


def resample_kline_all(exchange: ExchangeType, trade_type: TradeType, resample_interval: str, base_offset: str):
    """
    Resample kline data for all symbols of a given trade type.
    """
    logger.info(f"Resample kline {exchange.value} {trade_type.value} {resample_interval}")
    # 1. Get symbols & resampled directory
    if exchange == "binance":
        resampled_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / resample_interval / base_offset
        results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "1m"
    elif exchange == "bybit":
        resampled_dir = BYBIT_DATA_DIR / "results_data" / "linear" / resample_interval / base_offset
        results_dir = BYBIT_DATA_DIR / "results_data" / "linear" / "1m"
    elif exchange == "okx":
        resampled_dir = OKX_DATA_DIR / "results_data" / "swap" / resample_interval / base_offset
        results_dir = OKX_DATA_DIR / "results_data" / "swap" / "1m"
    symbols = sorted(p.stem for p in results_dir.glob("*.pqt"))

    # 2. Remove existing resampled directory
    if resampled_dir.exists():
        logger.debug(f"Resampled kline directory exists, removing it {resampled_dir}")
        shutil.rmtree(resampled_dir)

    # 3. Run resampling
    run_func = partial(
        resample_kline,
        exchange=exchange,
        trade_type=trade_type,
        resample_interval=resample_interval,
        base_offset=base_offset,
    )

    with ProcessPoolExecutor(max_workers=N_JOBS, mp_context=mp.get_context("spawn"), initializer=mp_env_init) as exe:
        tasks = [exe.submit(run_func, symbol=symbol) for symbol in symbols]
        now = datetime.now()
        with tqdm(total=len(tasks), ncols=100, desc=f"\033[92m{now.strftime('%H:%M:%S')}\033[0m | Resample |", colour="green") as pbar:
            for task in as_completed(tasks):
                symbol = task.result()
                pbar.set_postfix_str(symbol)
                pbar.update(1)
