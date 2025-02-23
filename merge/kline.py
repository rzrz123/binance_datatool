from datetime import timedelta
from typing import Optional

import polars as pl

from aws.kline.parse import TSManager
from config import BINANCE_DATA_DIR, TradeType


def merge_klines(
    trade_type: TradeType, symbol: str, time_interval: str, exclude_empty: bool
) -> Optional[pl.DataFrame]:
    """
    Merge K-line data from AWS parsed data and API downloaded data

    Args:
        trade_type: Trading type (e.g. SPOT, UM, CM)
        symbol: Trading pair symbol (e.g. BTCUSDT)
        time_interval: K-line interval (e.g. 1m, 1h)
        exclude_empty: Whether to exclude K-lines with zero volume

    Returns:
        Merged DataFrame containing data from both sources, or None if no AWS data found
    """
    # Get AWS parsed data directory
    parsed_symbol_kline_dir = (
        BINANCE_DATA_DIR
        / "parsed_data"
        / trade_type.value
        / "klines"
        / symbol
        / time_interval
    )

    # Get AWS parsed data
    ts_mgr = TSManager(parsed_symbol_kline_dir)
    aws_df = ts_mgr.read_all()

    if aws_df is None or aws_df.is_empty():
        return None

    if exclude_empty:
        aws_df = aws_df.filter(pl.col("volume") > 0)

    # Get API data directory
    api_kline_dir = (
        BINANCE_DATA_DIR
        / "api_data"
        / trade_type.value
        / "klines"
        / symbol
        / time_interval
    )

    # Read all API data files
    api_files = list(api_kline_dir.glob("*.pqt"))

    if not api_files:
        return aws_df

    # Read and concatenate all API data
    api_df = pl.read_parquet(api_files, columns=aws_df.columns)

    if exclude_empty:
        api_df = api_df.filter(pl.col("volume") > 0)

    # Merge the dataframes, keeping all rows from both sources
    merged_df = pl.concat([aws_df, api_df])

    # Remove duplicates and sort by timestamp
    merged_df = merged_df.unique(subset=["candle_begin_time"], keep="last").sort(
        "candle_begin_time"
    )

    return merged_df


def scan_gaps(df: pl.DataFrame, min_days: int, min_price_chg: float) -> pl.DataFrame:
    ldf = df.lazy()

    ldf = ldf.with_columns(
        pl.col("candle_begin_time").diff().alias("time_diff"),
        (pl.col("open") / pl.col("close").shift() - 1).alias("price_change"),
        pl.col("candle_begin_time").shift().alias("prev_begin_time"),
        pl.col('close').shift().alias('prev_close')
    )

    min_delta = timedelta(days=min_days)
    df_gap = ldf.filter((pl.col("time_diff") > min_delta) & (pl.col("price_change").abs() > min_price_chg))
    df_gap = df_gap.select("prev_begin_time", "candle_begin_time", 'prev_close', "open", "time_diff", "price_change")

    return df_gap.collect()