import click as click

from csdatatools.datasets.cincensus.cli import cin
from csdatatools.datasets.s903.s903_cli import s903
from csdatatools.datasets.annex_a.annex_a_cli import annex_a
from liia_pipeline.datasets.cin_census.cin_cli import cin_liia

@click.group()
def cli():
    pass


cli.add_command(cin)
cli.add_command(s903)
cli.add_command(annex_a)
cli.add_command(cin_liia)


if __name__ == '__main__':
    cli()