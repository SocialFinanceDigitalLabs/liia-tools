import click as click

from liiatools.datasets.annex_a.annex_a_cli import annex_a
from liiatools.datasets.cin_census.cin_cli import cin_census

@click.group()
def cli():
    pass


cli.add_command(annex_a)
cli.add_command(cin_census)

if __name__ == '__main__':
    cli()
