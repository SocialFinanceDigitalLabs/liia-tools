import click as click

from liiatools.datasets.annex_a.annex_a_cli import annex_a
from liiatools.datasets.cin_census.cin_cli import cin_census
from liiatools.datasets.s903.s903_cli import s903


@click.group()
def cli():
    pass


cli.add_command(annex_a)
cli.add_command(cin_census)
cli.add_command(s903)

if __name__ == "__main__":
    cli()
