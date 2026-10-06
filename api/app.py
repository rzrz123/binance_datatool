import asyncio
from typing import Annotated

import typer

from config import TradeType

from .binance import download_funding_rates_all

app = typer.Typer()


@app.command()
def download_recent_funding_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (um_futures/cm_futures)")],
):
    """
    Fill the open month of funding rates. AWS fundingRate files are monthly, so the current month is not on S3 yet.
    """
    asyncio.run(download_funding_rates_all(trade_type))
