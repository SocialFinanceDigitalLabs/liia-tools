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

# added to run from command line
@click_log.simple_verbosity_option(log)
def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input social_work_workforce xml files according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """
    output = csww_main_functions.cleanfile(input, la_code, la_log_dir, output)
    return output