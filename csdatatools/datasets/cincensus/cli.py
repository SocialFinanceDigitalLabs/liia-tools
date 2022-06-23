import logging
import shutil
from pathlib import Path

import click as click
import click_log

from csdatatools.datasets.cincensus import process
from csdatatools.datasets.cincensus.sample_data import generate_sample_census_file
from csdatatools.datasets.cincensus.schema import Schema
from csdatatools.util.stream import consume
from csdatatools.util.xml import etree, to_xml
from csdatatools.datasets.cincensus.validator import CinValidator
from csdatatools.spec import cin as cin_spec


logger = logging.getLogger()
click_log.basic_config(logger)


@click.group()
def cin():
    """CIN Census cleaner and validator"""
    pass


@cin.command()
@click.argument('filename', type=click.Path(exists=True))
@click_log.simple_verbosity_option(logger)
def validate(filename):
    """Validate a CIN Census file."""
    validator = CinValidator()
    validator.validate(filename)


@cin.command()
@click.argument('filename', type=click.Path(exists=True))
@click.option('-o', '--output', type=click.Path(exists=False))
@click_log.simple_verbosity_option(logger)
def validationreport(filename, output):
    schema = Schema()
    process.validationreport(filename, schema.schema, output)


@cin.command(name='remove-invalid')
@click.argument('filename', type=click.Path(exists=True))
@click.option('-o', '--output', type=click.Path(exists=False))
@click_log.simple_verbosity_option(logger)
def remove_invalid(filename, output):
    schema = Schema()
    process.remove_invalid_children(filename, schema.schema, output)


@cin.command(name='export-sample')
@click.argument('filename', type=click.Path(exists=False))
@click_log.simple_verbosity_option(logger)
def exportsample(filename):
    """Export a sample file for testing."""

    sample_file = Path(cin_spec.__file__).parent / 'samples/cin-2022.xml'
    assert sample_file.exists(), "Sample file not found"

    shutil.copy(sample_file, filename)
    click.echo(f"Sample file written to {filename}")


@cin.command(name='generate-sample')
@click.argument('filename', type=click.Path(exists=False))
@click_log.simple_verbosity_option(logger)
def gensample(filename):
    """Export a sample file for testing."""

    stream = generate_sample_census_file()
    builder = etree.TreeBuilder()
    stream = to_xml(stream, builder)
    consume(stream)

    element = builder.close()
    element = etree.tostring(element, encoding='utf-8', pretty_print=True)
    with open(filename, 'wb') as FILE:
        FILE.write(element)
