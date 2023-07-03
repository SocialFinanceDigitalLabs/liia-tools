import logging
import click_log
from pathlib import Path
from datetime import datetime
from liiatools.datasets.social_work_workforce.sample_data import (
    generate_sample_csww_file,
)
from liiatools.csdatatools.util.xml import dom_parse
from liiatools.csdatatools.util.stream import consume
from liiatools.csdatatools.util.xml import etree, to_xml
from liiatools.datasets.shared_functions.common import (
    # flip_dict,
    check_file_type,
    supported_file_types,
    check_year,
    check_year_within_range,
    save_year_error,
    save_incorrect_year_error,
)


def generate_sample(output: str):
    """
    Export a sample file for testing

    :param output: string containing the desired location and name of sample file
    :return: .xml sample file in desired location
    """

    stream = generate_sample_csww_file()
    builder = etree.TreeBuilder()
    stream = to_xml(stream, builder)
    consume(stream)

    element = builder.close()
    element = etree.tostring(element, encoding="utf-8", pretty_print=True)
    try:
        with open(output, "wb") as FILE:
            FILE.write(element)
    except FileNotFoundError:
        print("The file path provided does not exist")


def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input CIN Census xml files according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Open & Parse file
    if (
        check_file_type(
            input,
            file_types=[".xml"],
            supported_file_types=supported_file_types,
            la_log_dir=la_log_dir,
        )
        == "incorrect file type"
    ):
        return
    stream = dom_parse(input)
    stream = list(stream)

    # Get year from input file
    try:
        filename = str(Path(input).resolve().stem)
        input_year = check_year(filename)
    except (AttributeError, ValueError):
        save_year_error(input, la_log_dir)
        return

    # Check year is within acceptable range for data retention policy
    years_to_go_back = 6
    year_start_month = 6
    reference_date = datetime.now()
    if (
        check_year_within_range(
            input_year, years_to_go_back, year_start_month, reference_date
        )
        is False
    ):
        save_incorrect_year_error(input, la_log_dir)
        return
