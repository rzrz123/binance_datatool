import typer
from typing_extensions import Annotated

from config import TradeType

from .kline import gen_kline_type
from .resample import resample_kline_all

app = typer.Typer()


@app.command()
def kline_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    time_interval: Annotated[str, typer.Argument(help="K-line time interval, e.g., '1m', '5m', '1h'")],
    split_gaps: Annotated[bool, typer.Option(help="Whether to split data by gaps")] = False,
    min_days: Annotated[int, typer.Option(help="Minimum gap days threshold")] = 1,
    min_price_chg: Annotated[float, typer.Option(help="Minimum price change ratio threshold")] = 0.1,
):
    """
    Build klines from parsed AWS data for all symbols of a trade type and interval.
    VWAP is always written. Spot has no funding. UM and CM always merge the monthly AWS file with the API fill for the open month.
    """
    gen_kline_type(
        trade_type=trade_type,
        time_interval=time_interval,
        split_gaps=split_gaps,
        min_days=min_days,
        min_price_chg=min_price_chg,
    )


@app.command()
def resample_type(
    trade_type: Annotated[TradeType, typer.Argument(help="Type of trading (spot/futures)")],
    resample_interval: Annotated[str, typer.Argument(help="Resample interval, e.g., '1h', '4h'")],
    base_offset: Annotated[str, typer.Argument(help="Base offset, e.g., '5m', '15m', '30m'")],
):
    """
    Resample kline data for all symbols of given trade type and resample interval.
    """
    resample_kline_all(trade_type=trade_type, resample_interval=resample_interval, base_offset=base_offset)
