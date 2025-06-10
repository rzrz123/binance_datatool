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



def polars_calc_resample(exchange: ExchangeType, df: pl.DataFrame, resample_interval: str) -> pl.DataFrame:
    """
    Resample a Polars kline DataFrame to a higher time frame with an offset.
    For example, resample 5-minute klines to hourly klines with a 5-minute offset.

    Args:
        df: Polars kline DataFrame
        time_interval: Time interval of the klines
        resample_interval: Time interval to resample to
        offset_str: Offset to apply to the resampled klines

    Returns:
        Polars kline DataFrame
    """

    # Create a lazy DataFrame for efficient computation
    ldf = df.lazy()

    # Aggregation rules
    agg = [
        pl.col("symbol").last(),  # Symbol of the resampled kline
        pl.col("open").first(),  # Opening price of the resampled kline
        pl.col("high").max(),  # Highest price during the resampled period
        pl.col("low").min(),  # Lowest price during the resampled period
        pl.col("close").last(),  # Closing price of the resampled kline
        pl.col("volume").sum(),  # Total volume during the resampled period
        pl.col("quote_volume").sum(),  # Total quote volume during the resampled period
        pl.col("quote_volume").first().alias("quote_volume_algo"),  # Total quote volume during the resampled period
        pl.col("quote_volume").max().alias("quote_volume_max"),  # Total quote volume during the resampled period
    ]
    if exchange == "binance":
        agg += [
            pl.col("trade_num").sum(),  # Total number of trades during the resampled period
            pl.col("taker_buy_base_asset_volume").sum(),  # Total taker buy base asset volume during the resampled period
            pl.col("taker_buy_quote_asset_volume").sum(),  # Total taker buy quote asset volume during the resampled period
        ]

    if "avg_price_1m" in df.columns:
        agg.append(pl.col("avg_price_1m").first())

    if "funding_rate" in df.columns:
        # Only consider funding rates with absolute value greater than 0.01 bps
        has_funding_cond = pl.col("funding_rate").abs() > 1e-6
        # Get the first valid funding rate and its corresponding price and time
        agg.extend([
            pl.col("funding_rate").filter(has_funding_cond).first().alias("funding_rate"),
            pl.col("open").filter(has_funding_cond).first().alias("funding_price"),
            pl.col("candle_begin_time").filter(has_funding_cond).first().alias("funding_time")
        ])

    # Group the data by the start time of the klines, resampling to the specified interval with the given offset
    ldf = ldf.group_by_dynamic("candle_begin_time", every=resample_interval).agg(agg).fill_null(0)

    return ldf.collect()


def resample_kline(exchange: ExchangeType, trade_type: TradeType, symbol: str, resample_interval: str):
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

    # 2. Read kline data
    df = pl.read_parquet(results_dir / "1m" / f"{symbol}.pqt")

    # 3. Create output directory for this offset
    resampled_offset_dir = results_dir / resample_interval
    resampled_offset_dir.mkdir(parents=True, exist_ok=True)

    # 4. Read and resample data
    df_resampled = polars_calc_resample(exchange, df, resample_interval)
    df_resampled.write_parquet(resampled_offset_dir / f"{symbol}.pqt")

    return symbol


def resample_kline_all(exchange: ExchangeType, trade_type: TradeType, resample_interval: str):
    """
    Resample kline data for all symbols of a given trade type.
    """
    logger.info(f"Resample kline {exchange.value} {trade_type.value} {resample_interval}")
    # 1. Get symbols & resampled directory
    if exchange == "binance":
        resampled_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / resample_interval
        results_dir = BINANCE_DATA_DIR / "results_data" / trade_type.value / "1m"
    elif exchange == "bybit":
        resampled_dir = BYBIT_DATA_DIR / "results_data" / "linear" / resample_interval
        results_dir = BYBIT_DATA_DIR / "results_data" / "linear" / "1m"
    elif exchange == "okx":
        resampled_dir = OKX_DATA_DIR / "results_data" / "swap" / resample_interval
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
    )

    with ProcessPoolExecutor(max_workers=N_JOBS, mp_context=mp.get_context("spawn"), initializer=mp_env_init) as exe:
        tasks = [exe.submit(run_func, symbol=symbol) for symbol in symbols]
        now = datetime.now()
        with tqdm(total=len(tasks), ncols=100, desc=f"\033[92m{now.strftime('%H:%M:%S')}\033[0m | Resample |", colour="green") as pbar:
            for task in as_completed(tasks):
                symbol = task.result()
                pbar.set_postfix_str(symbol)
                pbar.update(1)
