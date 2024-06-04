import click as click

from liiatools.annex_a_pipeline.cli import annex_a
from liiatools.school_census_pipeline.cli import school_census
from liiatools.cin_census_pipeline.cli import cin_census
from liiatools.csww_pipeline.cli import csww
from liiatools.ssda903_pipeline.cli import s903
from liiatools.s251_pipeline.cli import s251
from liiatools.reformat_csv import reformat_csv


@click.group()
def cli():
    pass


cli.add_command(annex_a)
cli.add_command(cin_census)
cli.add_command(s903)
cli.add_command(csww)
cli.add_command(s251)
cli.add_command(school_census)
cli.add_command(reformat_csv)

if __name__ == "__main__":
    cli()
