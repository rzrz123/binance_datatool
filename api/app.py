import asyncio
import os
from typing import Optional

import typer
from typing_extensions import Annotated

from config import TradeType

from .binance import (
    api_download_kline,
    download_funding_rates_all,
    download_missing_kline_type,
)
from .bybit import download_bybit_linear_funding_rates, download_bybit_linear_klines

app = typer.Typer()

HTTP_PROXY = os.getenv("HTTP_PROXY", None) or os.getenv("http_proxy", None)

@app.command()
def download_kline(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[str, typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'.")],
    symbol: Annotated[str, typer.Argument(help="A trading symbol, e.g., 'BTCUSDT' or 'ETHUSDT'.")],
    dts: Annotated[list[str], typer.Argument(help="A list trading dates, e.g., '20200101 20210203'.")],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance klines for specific symbol and dates from Kline API
    """
    sym_dts = [(symbol, dt) for dt in dts]
    asyncio.run(api_download_kline(trade_type, time_interval, sym_dts, http_proxy))


@app.command()
def download_aws_missing_kline_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_interval: Annotated[str, typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'.")],
    overwrite: Annotated[bool, typer.Option(help="Whether to overwrite existing files")] = False,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance kline data from the Kline API for the provided trade_type that have missing dates in AWS
    """
    asyncio.run(download_missing_kline_type(trade_type, time_interval, overwrite, http_proxy))


@app.command()
def download_recent_funding_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    """
    Download Binance funding rate for all symbols of a specific trade type from Binance API
    """
    asyncio.run(download_funding_rates_all(trade_type, http_proxy))


@app.command()
def download_bybit_klines_type():
    """
    Download Bybit klines for all symbols from Bybit API
    """
    asyncio.run(download_bybit_linear_klines())

@app.command()
def download_bybit_linear_funding_rates_type():
    """
    Download Bybit funding rates for all symbols from Bybit API
    """
    asyncio.run(download_bybit_linear_funding_rates())
