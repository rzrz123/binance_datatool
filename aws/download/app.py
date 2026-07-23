import asyncio

import typer
from typing_extensions import Annotated

from aws.download.download import (
    download_cm_funding_rates,
    download_cm_klines,
    download_spot_klines,
    download_um_funding_rates,
    download_um_klines,
)
from config import ContractType

app = typer.Typer()


@app.command('um-funding')
def um_funding(
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = 'USDT',
    contract_type: Annotated[
        ContractType, typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'.")
    ] = ContractType.perpetual,
):
    '''Download and verify Binance USDⓈ-M Futures funding rates'''
    asyncio.run(download_um_funding_rates(quote, contract_type))


@app.command('cm-funding')
def cm_funding(
    contract_type: Annotated[
        ContractType, typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'.")
    ] = ContractType.perpetual,
):
    '''Download and verify Binance Coin Futures funding rates'''
    asyncio.run(download_cm_funding_rates(contract_type))


@app.command('spot-klines')
def spot_klines(
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
):
    '''Download and verify Binance spot klines'''
    for time_interval in time_intervals:
        asyncio.run(download_spot_klines(time_interval, quote, stablecoins, leverage_coins))


@app.command('um-klines')
def um_klines(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    quote: Annotated[str, typer.Option(help="The quote currency, e.g., 'USDT', 'USDC', 'BTC'.")] = 'USDT',
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
):
    '''Download and verify Binance USDⓈ-M Futures klines'''
    for time_interval in time_intervals:
        asyncio.run(download_um_klines(time_interval, quote, contract_type))


@app.command('cm-klines')
def cm_klines(
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    contract_type: Annotated[
        ContractType,
        typer.Option(help="The type of contract, 'PERPETUAL' or 'DELIVERY'."),
    ] = ContractType.perpetual,
):
    '''Download and verify Binance COIN-M Futures klines'''
    for time_interval in time_intervals:
        asyncio.run(download_cm_klines(time_interval, contract_type))
