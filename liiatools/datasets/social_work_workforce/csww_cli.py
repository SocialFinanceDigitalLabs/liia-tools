import logging
import click as click
import click_log

from liiatools.datasets.social_work_workforce import csww_main_functions


logger = logging.getLogger()
click_log.basic_config(logger)


@click.group()
def csww():
    """
    Functions for creating CSWW Census sample file generator
    """
    pass


@csww.command()
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output file location, including the file name and suffix",
)
@click_log.simple_verbosity_option(logger)
def generate_sample(output: str):
    """
    Export a sample file for testing

    :param output: string containing the desired location and name of sample file
    :return: .xml sample file in desired location
    """
    output = csww_main_functions.generate_sample(output)
    return output


@csww.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output directory location",
)
def la_agg(input, output):
    """
    Joins data from newly cleaned CSWW files (output of cleanfile()) to existing CSWW files data for the depositing local authority
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param output: should specify the path to the output folder
    :return: None
    """
    csww_main_functions.la_agg(input, output)
