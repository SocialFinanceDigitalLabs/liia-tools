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
from liiatools.csdatatools.datasets.social_work_workforce import filters
from liiatools.datasets.social_work_workforce.lds_csww_clean.schema import Schema
from liiatools.datasets.shared_functions.common import (
    flip_dict,
    check_file_type,
    supported_file_types,
    check_year,
    check_year_within_range,
    save_year_error,
    save_incorrect_year_error,
)
from liiatools.datasets.social_work_workforce.lds_csww_clean import (
    configuration as clean_config,
    csww_record,
    file_creator,
)

# from liiatools.datasets.social_work_workforce.lds_csww_la_agg import (
#     configuration as agg_config,
# )
# from liiatools.datasets.social_work_workforce.lds_csww_la_agg import (
#     process as agg_process,
# )

from liiatools.datasets.social_work_workforce.lds_csww_la_agg import (
    configuration as agg_config,
)
from liiatools.datasets.social_work_workforce.lds_csww_la_agg import (
    process as agg_process,
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
    Cleans input CSWW xml files according to config and outputs cleaned csv files.
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
    years_to_go_back = 7
    year_start_month = 1
    reference_date = datetime.now()
    if (
        check_year_within_range(
            input_year, years_to_go_back, year_start_month, reference_date
        )
        is False
    ):
        save_incorrect_year_error(input, la_log_dir)
        return

    # Configure stream
    config = clean_config.Config()
    la_name = flip_dict(config["data_codes"])[la_code]
    stream = filters.strip_text(stream)
    stream = filters.add_context(stream)
    stream = filters.add_schema(stream, schema=Schema(input_year).schema)

    # Output result
    stream = csww_record.message_collector(stream)

    data_wklevel, data_lalevel = csww_record.export_table(stream)

    data_wklevel = file_creator.add_fields(input_year, data_wklevel, la_name, la_code)
    file_creator.export_file(input, output, data_wklevel, "workerlevel")

    data_lalevel = file_creator.add_fields(input_year, data_lalevel, la_name, la_code)
    file_creator.export_file(input, output, data_lalevel, "lalevel")


def la_agg(input, output):
    """
    Joins data from newly cleaned CSWW file (output of cleanfile()) to existing CSWW data for the depositing local authority
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = agg_config.Config()

    # Open file as DataFrame and match file type
    csww_df = agg_process.read_file(input)
    column_names = config["column_names"]
    table_name = agg_process.match_load_file(csww_df, column_names)

    # Merge file with existing file of the same type in LA output folder
    csww_df = agg_process.merge_la_files(output, csww_df, table_name)

    # De-duplicate and remove old data according to schema
    dates = config["dates"]
    csww_df = agg_process.convert_datetimes(csww_df, dates, table_name)
    sort_order = config["sort_order"]
    dedup = config["dedup"]
    csww_df = agg_process.deduplicate(s903_df, table_name, sort_order, dedup)
    csww_df = agg_process.remove_old_data(
        csww_df,
        num_of_years= 7,
        new_year_start_month= 1,
        as_at_date= datetime.now(),
    )

    # If file still has data, after removing old data: re-format and export merged file
    if len(csww_df) > 0:
        csww_df = agg_process.convert_dates(csww_df, dates, table_name)
        agg_process.export_la_file(output, table_name, csww_df)


# cleanfile(
#     "/workspaces/liia-tools/liiatools/spec/social_work_workforce/samples/csww/BAD/social_work_workforce_2022.xml",
#     "BAD",
#     "/workspaces/liia_tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
# )
