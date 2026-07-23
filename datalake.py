import typer

from aws.download.app import app as aws_download
from aws.parse.app import app as aws_parse
from api.app import app as api_data

from generate.app import app as generate

app = typer.Typer()

app.add_typer(
    aws_download,
    name='aws_download',
    help='Download and verify Binance AWS data.',
)
app.add_typer(
    aws_parse,
    name='aws_parse',
    help='Parse Binance AWS data into parquet.',
)
app.add_typer(
    api_data,
    name='api_data',
    help='Commands for maintaining Binance API data.',
)
app.add_typer(
    generate,
    name='generate',
    help='Commands to generate the resulting data.',
)


if __name__ == '__main__':
    app()
