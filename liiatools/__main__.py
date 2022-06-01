import click as click

from liiatools.datasets.annex_a.annex_a_cli import annex_a

@click.group()
def cli():
    pass

cli.add_command(annex_a)

if __name__ == '__main__':
    cli()
