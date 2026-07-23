import typer

from src.aws_clone.app import app as clone_app
from src.aws_parse.app import app as parse_app

app = typer.Typer(
    add_completion=False,
    help='Unified entry for src/ tools (aws clone + ArcticDB parse).',
)
app.add_typer(clone_app, name='clone', help='Clone Binance Vision S3 prefixes to local disk.')
app.add_typer(parse_app, name='parse', help='Parse verified zips into ArcticDB / merge wide table.')

if __name__ == '__main__':
    app()
