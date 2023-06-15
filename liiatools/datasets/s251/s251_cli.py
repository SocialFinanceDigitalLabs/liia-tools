import logging
import click as click
import click_log

from liiatools.datasets.s251 import s251_main_functions


logger = logging.getLogger()
click_log.basic_config(logger)


@click.group()
def s251():
    """
    Functions for creating S251 sample file generator
    """
    pass


@s251.command()
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output file location, including the file name and suffix",
)
@click_log.simple_verbosity_option(logger)
def generate_sample(la_code, output):
    """
    Export a sample file for testing

    :param la_code: should be a three-letter string for the local authority depositing the file
    :param output: string containing the desired location and name of sample file
    :return: .csv sample file in desired location
    """
    output = s251_main_functions.generate_sample(la_code, output)
    return output
