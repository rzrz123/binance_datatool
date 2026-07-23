"""Typer CLI for ArcticDB parse / merge."""

from pathlib import Path

import typer
from typing_extensions import Annotated

from src.paths import DEFAULT_ARCTIC_URI, DEFAULT_AWS_DATA_DIR, TradeType

app = typer.Typer(add_completion=False, help='Parse verified AWS zips into ArcticDB (two-step + wide merge).')


@app.command('klines')
def parse_klines_cmd(
    trade_type: Annotated[TradeType, typer.Option(help='spot | um_futures | cm_futures')],
    interval: Annotated[str, typer.Option(help='Kline interval, e.g. 1m')] = '1m',
    force: Annotated[bool, typer.Option(help='Re-ingest days already present in Arctic')] = False,
    symbols: Annotated[str | None, typer.Option(help='Comma-separated symbol filter, e.g. BTCUSDT,ETHUSDT')] = None,
    aws_data_dir: Annotated[Path, typer.Option(help='Local aws_data root')] = DEFAULT_AWS_DATA_DIR,
    arctic_uri: Annotated[str, typer.Option(help='ArcticDB URI')] = DEFAULT_ARCTIC_URI,
):
    """Step1a: verified daily kline zips -> Arctic klines_{interval}."""
    from .parse_klines import parse_klines

    sym_list = [s.strip() for s in symbols.split(',')] if symbols else None
    result = parse_klines(trade_type.value, interval, aws_data_dir=aws_data_dir, arctic_uri=arctic_uri, force=force, symbols=sym_list)
    typer.echo(result)


@app.command('funding')
def parse_funding_cmd(
    trade_type: Annotated[TradeType, typer.Option(help='um_futures | cm_futures (spot skipped)')],
    force: Annotated[bool, typer.Option(help='Re-ingest months already present in Arctic')] = False,
    symbols: Annotated[str | None, typer.Option(help='Comma-separated symbol filter')] = None,
    aws_data_dir: Annotated[Path, typer.Option(help='Local aws_data root')] = DEFAULT_AWS_DATA_DIR,
    arctic_uri: Annotated[str, typer.Option(help='ArcticDB URI')] = DEFAULT_ARCTIC_URI,
):
    """Step1b: verified monthly funding zips + current-month API -> Arctic funding."""
    from .parse_funding import parse_funding

    sym_list = [s.strip() for s in symbols.split(',')] if symbols else None
    result = parse_funding(trade_type.value, aws_data_dir=aws_data_dir, arctic_uri=arctic_uri, force=force, symbols=sym_list)
    typer.echo(result)


@app.command('merge-wide')
def merge_wide_cmd(
    trade_type: Annotated[TradeType, typer.Option(help='spot | um_futures | cm_futures')],
    interval: Annotated[str, typer.Option(help='Kline interval, e.g. 1m')] = '1m',
    force: Annotated[bool, typer.Option(help='Rebuild wide days already present')] = False,
    symbols: Annotated[str | None, typer.Option(help='Comma-separated symbol filter')] = None,
    aws_data_dir: Annotated[Path, typer.Option(help='Local aws_data root (for day calendar)')] = DEFAULT_AWS_DATA_DIR,
    arctic_uri: Annotated[str, typer.Option(help='ArcticDB URI')] = DEFAULT_ARCTIC_URI,
):
    """Step2: left-join funding onto klines -> Arctic klines_{interval}_wide."""
    from .merge_wide import merge_wide

    sym_list = [s.strip() for s in symbols.split(',')] if symbols else None
    result = merge_wide(trade_type.value, interval, aws_data_dir=aws_data_dir, arctic_uri=arctic_uri, force=force, symbols=sym_list)
    typer.echo(result)
