import asyncio
import os
from typing import Optional

import typer
from typing_extensions import Annotated

from aws.kline.download import download_cm_klines, download_klines, download_spot_klines,download_um_klines
from aws.kline.verify import verify_klines, verify_all_klines
from aws.kline.parse import parse_klines, parse_all_klines
from config import ContractType, TradeType

app = typer.Typer()

HTTP_PROXY = os.getenv('HTTP_PROXY', None) or os.getenv('http_proxy', None)


@app.command()
def download_spot(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = 'USDT',
    stablecoins: Annotated[
        bool,
        typer.Option(help="Whether to include stablecoin symbols, such as 'USDCUSDT'."),
    ] = False,
    leverage_coins: Annotated[
        bool,
        typer.Option(help="Whether to include leveraged coin symbols, such as 'BTCUPUSDT'."),
    ] = False,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance spot klines
    '''
    for time_interval in time_intervals:
        asyncio.run(download_spot_klines(time_interval, quote, stablecoins, leverage_coins, http_proxy))


@app.command()
def download_um_futures(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = 'USDT',
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance USDⓈ-M Futures klines
    '''
    for time_interval in time_intervals:
        asyncio.run(download_um_klines(time_interval, quote, contract_type, http_proxy))


@app.command()
def download_cm_futures(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
    http_proxy: Annotated[Optional[str], typer.Option(help="HTTP proxy address")] = HTTP_PROXY,
):
    '''
    Download Binance COIN-M Futures klines
    '''
    for time_interval in time_intervals:
        asyncio.run(download_cm_klines(time_interval, contract_type, http_proxy))


@app.command()
def verify_type_all(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
):
    '''
    Verify Binance Klines for all symbols with the given trade type and time intervals
    '''
    for time_interval in time_intervals:
        verify_all_klines(trade_type, time_interval)


@app.command()
def parse_type_all(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of symbols")],
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    force_update: bool = False
):
    '''
    Parse Binance Klines for all symbols with the given trade type and time intervals
    '''
    for time_interval in time_intervals:
        parse_all_klines(trade_type, time_interval, force_update)
