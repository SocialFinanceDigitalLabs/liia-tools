from pathlib import Path
import yaml
import logging
import click_log
from datetime import datetime

from liiatools.datasets.s251.lds_s251_clean import (
    configuration as clean_config,
    parse,
    populate,
    filters,
    degrade,
    logger,
    file_creator,
    prep,
)
from liiatools.datasets.s251.data_generator import sample_data

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import (
    flip_dict,
    check_file_type,
    supported_file_types,
    check_year,
    save_year_error,
    save_incorrect_year_error,
    check_year_within_range
)

log = logging.getLogger()
click_log.basic_config(log)

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())


def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input S251 csv file according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Prepare and check file
    if prep.check_blank_file(input, la_log_dir=la_log_dir) == "empty":
        return
    prep.drop_empty_rows(input, input)
    if (
            check_file_type(
                input,
                file_types=[".csv"],
                supported_file_types=supported_file_types,
                la_log_dir=la_log_dir,
            )
            == "incorrect file type"
    ):
        return

    # Open & Parse file
    stream = parse.parse_csv(input=input)
    year = populate.find_year_of_return(stream)
    print(stream)
    # stream = populate.add_year_column(stream, year=year)
    #
    # # Configure stream
    # config = clean_config.Config(year)
    # la_name = flip_dict(config["data_codes"])[la_code]
    # stream = clean_config.configure_stream(stream, config)
    #
    # # Clean stream
    # stream = filters.clean(stream)
    # stream = degrade.degrade(stream)
    # stream = logger.log_errors(stream)
    # stream = populate.create_la_child_id(stream, la_code=la_code)
    #
    # # Output result
    # stream = file_creator.save_stream(stream, la_name=la_name, output=output)
    # stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    for e in stream:
        print(e)
    list(stream)


cleanfile(r"C:\Users\patrick.troy\Downloads\LIIA tests\s251_test.csv", "BAR",
          r"C:\Users\patrick.troy\Downloads\LIIA tests", r"C:\Users\patrick.troy\Downloads\LIIA tests")


def generate_sample(output):
    """
    Export a sample file for testing

    :param output: string containing the desired location and name of sample file
    :return: .csv sample file in desired location
    """
    stream = sample_data.generate_sample_s251_file()
    stream = file_creator.save_stream(stream, output=output)
    list(stream)
