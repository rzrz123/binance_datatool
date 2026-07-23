import asyncio
from pathlib import Path

import typer
from typing_extensions import Annotated

from src.paths import DEFAULT_AWS_DATA_DIR

app = typer.Typer(add_completion=False, help='Clone Binance Vision S3 prefixes to local disk.')


@app.callback(invoke_without_command=True)
def clone(
    ctx: typer.Context,
    prefix: Annotated[str, typer.Argument(help="S3 prefix, e.g. data/futures/um/daily/klines/*/1m/")],
    output_dir: Annotated[Path, typer.Option(help='Local aws_data root')] = DEFAULT_AWS_DATA_DIR,
):
    """Clone a Binance Vision prefix and verify SHA256 checksums."""
    if ctx.invoked_subcommand is not None:
        return

    from .clone import clone as run_clone

    result = asyncio.run(run_clone(prefix, output_dir=output_dir))
    typer.echo(
        f'listed={result.listed} already_present={result.already_present} '
        f'to_download={result.to_download} missing={result.missing_after_download} '
        f'verified_ok={result.verified_ok} verified_fail={result.verified_fail}'
    )
    if result.missing_after_download or result.verified_fail:
        raise typer.Exit(code=1)
