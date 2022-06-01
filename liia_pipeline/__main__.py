import click as click

from liia_pipeline.datasets.cin_census.cin_cli import cin

@click.group()
def cli():
    pass


cli.add_command(cin)


if __name__ == '__main__':
    cli()
