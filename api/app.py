import asyncio

import typer
from typing_extensions import Annotated

from config import TradeType

from .binance import (
    api_download_kline,
    download_funding_rates_all,
    download_missing_kline_type,
)

app = typer.Typer()


@app.command()
def download_kline(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[str, typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'.")],
    symbol: Annotated[str, typer.Argument(help="A trading symbol, e.g., 'BTCUSDT' or 'ETHUSDT'.")],
    dts: Annotated[list[str], typer.Argument(help="A list trading dates, e.g., '20200101 20210203'.")],
):
    """
    Download Binance klines for specific symbol and dates from Kline API
    """
    sym_dts = [(symbol, dt) for dt in dts]
    asyncio.run(api_download_kline(trade_type, time_interval, sym_dts))


@app.command()
def download_aws_missing_kline_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[str, typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'.")],
    overwrite: Annotated[bool, typer.Option(help="Whether to overwrite existing files")] = False,
):
    """
    Download Binance kline data from the Kline API for the provided trade_type that have missing dates in AWS
    """
    asyncio.run(download_missing_kline_type(trade_type, time_interval, overwrite))


@app.command()
def download_recent_funding_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
):
    """
    Download Binance funding rate for all symbols of a specific trade type from Binance API
    """
    asyncio.run(download_funding_rates_all(trade_type))
