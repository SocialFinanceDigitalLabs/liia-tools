from pathlib import Path
import yaml
import logging
import click_log
from datetime import datetime

# dependencies for cleanfile()
from liiatools.datasets.s903.lds_ssda903_clean import (
    configuration as clean_config,
    populate,
    filters,
    degrade,
    logger,
    file_creator,
)

# dependencies for la_agg()
from liiatools.datasets.s903.lds_ssda903_la_agg import configuration as agg_config

# dependencies for pan_agg()
from liiatools.datasets.s903.lds_ssda903_pan_agg import configuration as pan_config

# dependencies for sufficiency_output()
from liiatools.datasets.s903.lds_ssda903_sufficiency import configuration as suff_config
from liiatools.datasets.s903.lds_ssda903_sufficiency import process as suff_process

# dependencies for episodes fix()
from liiatools.datasets.s903.lds_ssda903_episodes_fix import process as episodes_process

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions import (
    prep,
    common,
    parse,
    process as common_process,
)

log = logging.getLogger()
click_log.basic_config(log)

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())
YEARS_TO_GO_BACK = 7
YEAR_START_MONTH = 1
REFERENCE_DATE = datetime.now()


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
    if prep.check_blank_file(input, la_log_dir=la_log_dir) == "empty":
        return
    prep.drop_empty_rows(input, input)

    # Configuration
    try:
        filename = str(Path(input).resolve().stem)
        year = common.check_year(filename)
    except (AttributeError, ValueError):
        common.save_year_error(input, la_log_dir)
        return

    if (
        common.check_year_within_range(
            year, YEARS_TO_GO_BACK, YEAR_START_MONTH, REFERENCE_DATE
        )
        is False
    ):
        common.save_incorrect_year_error(
            input, la_log_dir, retention_period=YEARS_TO_GO_BACK - 1
        )
        return

    config = clean_config.Config(year)
    la_name = common.flip_dict(config["data_codes"])[la_code]
    if (
        common.check_file_type(
            input,
            file_types=[".csv"],
            supported_file_types=common.supported_file_types,
            la_log_dir=la_log_dir,
        )
        == "incorrect file type"
    ):
        return

    # Open & Parse file
    stream = parse.parse_csv(input=input)
    stream = populate.add_year_column(stream, year)

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
    s903_df = common_process.read_file(input)
    column_names = config["column_names"]
    table_name = common_process.match_load_file(s903_df, column_names)

    # Merge file with existing file of the same type in LA output folder
    s903_df = common_process.merge_la_files(
        output, s903_df, table_name, filename="SSDA903"
    )

    # De-duplicate and remove old data according to schema
    dates = config["dates"]
    s903_df = common_process.convert_datetimes(s903_df, dates, table_name)
    sort_order = config["sort_order"]
    dedup = config["dedup"]
    s903_df = common_process.deduplicate(s903_df, table_name, sort_order, dedup)
    s903_df = common_process.remove_old_data(
        s903_df,
        num_of_years=YEARS_TO_GO_BACK,
        new_year_start_month=YEAR_START_MONTH,
        as_at_date=REFERENCE_DATE,
        year_column="YEAR",
    )

    # If file still has data, after removing old data: re-format and export merged file
    if len(s903_df) > 0:
        s903_df = common_process.convert_dates(s903_df, dates, table_name)
        common_process.export_la_file(output, table_name, s903_df, filename="SSDA903")


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
    s903_df = common_process.read_file(input)
    column_names = config["column_names"]
    table_name = common_process.match_load_file(s903_df, column_names)

    # Remove unwanted datasets and merge wanted with existing output
    pan_data_kept = config["pan_data_kept"]
    if table_name in pan_data_kept:
        la_name = common.flip_dict(config["data_codes"])[la_code]
        s903_df = common_process.merge_agg_files(
            output, table_name, s903_df, la_name, filename="SSDA903"
        )
        common_process.export_pan_file(output, table_name, s903_df, filename="SSDA903")


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
    s903_df = common_process.read_file(input)
    column_names = config["column_names"]
    table_name = suff_process.match_load_file(s903_df, column_names)

    # Minimises output following schema and exports file
    suff_data_kept = config["suff_data_kept"]
    if table_name in suff_data_kept:
        minimise = config["minimise"]
        s903_df = suff_process.data_min(s903_df, minimise, table_name)
        suff_process.export_suff_file(output, table_name, s903_df)


def episodes_fix(input, output):
    """"
    Applies fixes to la_agg SSDA903 Episodes files
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = agg_config.Config()

    # Read file and match type
    s903_df = common_process.read_file(input)
    column_names = config["column_names"]
    table_name = common_process.match_load_file(s903_df, column_names)
    
    # Process stage 1 rule fixes for Episodes table
    if table_name == "Episodes":
        # Add columns to dataframe to identify which rules should be applied
        s903_df = s903_df.sort_values(["CHILD", "DECOM"], ignore_index=True)
        s903_df_stage1 = episodes_process.create_previous_and_next_episode(s903_df, episodes_process.__COLUMNS)
        s903_df_stage1 = episodes_process.format_datetime(s903_df_stage1, episodes_process.__DATES)
        s903_df_stage1 = episodes_process.add_latest_year_and_source_for_la(s903_df_stage1)
        s903_df_stage1 = episodes_process.add_stage1_rule_identifier_columns(s903_df_stage1)
        s903_df_stage1 = episodes_process.identify_stage1_rule_to_apply(s903_df_stage1)

        # Apply the stage 1 rules
        s903_df_stage1_applied = episodes_process.apply_stage1_rules(s903_df_stage1)
        
        # Following code used to test outputs during development
        print("Dataframe with rules identified:")
        print(s903_df_stage1[["CHILD", "YEAR", "DECOM", "DEC", "Has_open_episode_error", "Rule_to_apply"]])
        print("Dataframe with stage 1 rules applied (Incomplete - more rules to apply):")
        print(s903_df_stage1_applied[["CHILD", "YEAR", "DECOM", "DEC", "Has_open_episode_error", "Rule_to_apply"]])

        s903_df_stage1_applied = s903_df_stage1_applied.sort_values(["CHILD", "DECOM"], ignore_index=True)
        s903_df_stage1_applied.to_csv(r"liiatools/datasets/s903/lds_ssda903_episodes_fix/SSDA903_episodes_for_testing_fixes_OUTPUT.csv",
                            index=False)

# Run episodes_fix() with our test file which contains examples of each rule (CHILD id indicates which rule)
episodes_fix(
    r"liiatools/datasets/s903/lds_ssda903_episodes_fix/SSDA903_episodes_for_testing_fixes_INPUT.csv",
    r"liiatools/datasets/s903/lds_ssda903_episodes_fix"
)

# poetry run python liiatools/datasets/s903/s903_main_functions.py
