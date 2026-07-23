import typer
from typing_extensions import Annotated

from aws.parse.funding import parse_funding_rates_all
from aws.parse.kline import parse_all_klines
from config import TradeType

app = typer.Typer()


@app.command()
def funding(
    trade_type: Annotated[TradeType, typer.Argument(help='Type of symbols')],
):
    '''Parse Binance funding rates for all symbols with the given trade type'''
    parse_funding_rates_all(trade_type)


@app.command()
def klines(
    trade_type: Annotated[TradeType, typer.Argument(help='Type of symbols')],
    time_intervals: Annotated[
        list[str],
        typer.Argument(help="The time interval for the K-lines, e.g., '1m', '5m', '1h'."),
    ],
    force_update: bool = False,
):
    '''Parse Binance Klines for all symbols with the given trade type and time intervals'''
    for time_interval in time_intervals:
        parse_all_klines(trade_type, time_interval, force_update)
