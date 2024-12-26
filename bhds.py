import typer

from aws.funding.app import app as aws_funding
from aws.kline.app import app as aws_kline

app = typer.Typer()

app.add_typer(aws_funding, name='aws_funding', help='Commands for maintaining Binance AWS funding rate data.')
app.add_typer(aws_kline, name='aws_kline', help='Commands for maintaining Binance AWS K-line data.')

if __name__ == '__main__':
    app()
