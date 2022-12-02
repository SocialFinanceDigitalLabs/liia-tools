from pathlib import Path
import yaml
import logging
import click_log

# dependencies for cleanfile()
from liiatools.datasets.s903.lds_ssda903_clean import (
    configuration as clean_config,
    parse,
    populate,
    filters,
    degrade,
    logger,
    file_creator,
    prep,
)

# dependencies for la_agg()
from liiatools.datasets.s903.lds_ssda903_la_agg import configuration as agg_config
from liiatools.datasets.s903.lds_ssda903_la_agg import process as agg_process

# dependencies for pan_agg()
from liiatools.datasets.s903.lds_ssda903_pan_agg import configuration as pan_config
from liiatools.datasets.s903.lds_ssda903_pan_agg import process as pan_process

# dependencies for sufficiency_output()
from liiatools.datasets.s903.lds_ssda903_sufficiency import configuration as suff_config
from liiatools.datasets.s903.lds_ssda903_sufficiency import process as suff_process

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import (
    flip_dict,
    check_file_type,
    supported_file_types,
)

log = logging.getLogger()
click_log.basic_config(log)

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())


def cleanfile(input, la_code, la_log_dir, output):
    """
    Cleans input SSDA903 csv files according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Prepare file
    # List of commonly submitted unneeded files
    drop_file_list = [
        "Extended Review",
        "Pupil Premium Children",
        "Children Ceasing to be looked after for other reasons",
        "Distance and Placement Extended",
        "Extended Adoption",
        "Children Ceased Care During the Year",
        "Children Looked After on 31st March",
        "Children Started Care During the Year",
    ]
    prep.delete_unrequired_files(
        input, drop_file_list=drop_file_list, la_log_dir=la_log_dir
    )
    if prep.check_blank_file(input, la_log_dir=la_log_dir) == "empty":
        return
    prep.drop_empty_rows(input, input)

    # Configuration
    config = clean_config.Config()
    la_name = flip_dict(config["data_codes"])[la_code]
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
    stream = populate.add_year_column(stream, input=input, la_log_dir=la_log_dir)

    # Configure stream
    stream = clean_config.configure_stream(stream, config)

    # Clean stream
    stream = filters.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, la_code=la_code)

    # Output result
    stream = file_creator.save_stream(stream, la_name=la_name, output=output)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    list(stream)


def la_agg(input, output):
    """
    Joins data from newly cleaned SSDA903 file (output of cleanfile()) to existing SSDA903 data for the depositing local authority
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = agg_config.Config()

    # Open file as DataFrame and match file type
    s903_df = agg_process.read_file(input)
    column_names = config["column_names"]
    table_name = agg_process.match_load_file(s903_df, column_names)

    # Merge file with existing file of the same type in LA output folder
    s903_df = agg_process.merge_la_files(output, s903_df, table_name)

    # De-duplicate and remove old data according to schema
    dates = config["dates"]
    s903_df = agg_process.convert_datetimes(s903_df, dates, table_name)
    sort_order = config["sort_order"]
    dedup = config["dedup"]
    s903_df = agg_process.deduplicate(s903_df, table_name, sort_order, dedup)
    s903_df = agg_process.remove_old_data(s903_df, years=6)

    # If file still has data, after removing old data: re-format and export merged file
    if len(s903_df) > 0:
        s903_df = agg_process.convert_dates(s903_df, dates, table_name)
        agg_process.export_la_file(output, table_name, s903_df)


def pan_agg(input, la_code, output):
    """
    Joins data from newly merged SSDA903 file (output of la-agg()) to existing pan-London SSDA903 data
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = pan_config.Config()

    # Read file and match type
    s903_df = pan_process.read_file(input)
    column_names = config["column_names"]
    table_name = pan_process.match_load_file(s903_df, column_names)

    # Remove unwanted datasets and merge wanted with existing output
    pan_data_kept = config["pan_data_kept"]
    if table_name in pan_data_kept:
        la_name = flip_dict(config["data_codes"])[la_code]
        s903_df = pan_process.merge_agg_files(output, table_name, s903_df, la_name)
        pan_process.export_pan_file(output, table_name, s903_df)


def sufficiency_output(input, output):
    """
    Applies data minimisation to data from aggregated SSDA903 file (output of pan-agg()) and outputs to Sufficiency analysis folder
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = suff_config.Config()

    # Read file and match type
    s903_df = suff_process.read_file(input)
    column_names = config["column_names"]
    table_name = suff_process.match_load_file(s903_df, column_names)

    # Minimises output following schema and exports file
    suff_data_kept = config["suff_data_kept"]
    if table_name in suff_data_kept:
        minimise = config["minimise"]
        s903_df = suff_process.data_min(s903_df, minimise, table_name)
        suff_process.export_suff_file(output, table_name, s903_df)
