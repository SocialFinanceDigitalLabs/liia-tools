from pathlib import Path
from datetime import datetime
import yaml

from liiatools.datasets.social_work_workforce.lds_csww_data_generator.sample_data import generate_sample_csww_file
from liiatools.datasets.social_work_workforce.lds_csww_data_generator.stream import consume

# Dependencies for cleanfile()
from liiatools.datasets.social_work_workforce.lds_csww_clean.parse import (
    etree,
    to_xml,
    dom_parse,
)
from liiatools.datasets.social_work_workforce.lds_csww_clean.schema import (
    Schema,
    FilePath,
)

from liiatools.datasets.social_work_workforce.lds_csww_clean import (
    file_creator,
    configuration as clean_config,
    csww_record,
    cleaner,
    logger,
    filters,
)

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import (
    flip_dict,
    check_file_type,
    supported_file_types,
    check_year,
    check_year_within_range,
    save_year_error,
    save_incorrect_year_error,
)

# dependencies for la_agg()
from liiatools.datasets.social_work_workforce.lds_csww_la_agg import (
    configuration as agg_config,
    process as agg_process,
)

# dependencies for pan_agg()
from liiatools.datasets.social_work_workforce.lds_csww_pan_agg import (
    configuration as pan_config,
    process as pan_process,
)


COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())

# Set constants for data retention period
YEARS_TO_GO_BACK = 7
YEAR_START_MONTH = 1
REFERENCE_DATE = datetime.now()


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
    Cleans input Children Social Work workforce xml files according to config and outputs cleaned csv files.
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

    # Get year from input file
    filename = str(Path(input).resolve().stem)
    try:
        input_year = check_year(filename)
    except (AttributeError, ValueError):
        save_year_error(input, la_log_dir)
        return

    # Check year is within acceptable range for data retention policy
    if (
        check_year_within_range(
            input_year, YEARS_TO_GO_BACK, YEAR_START_MONTH, REFERENCE_DATE
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
    stream = filters.add_schema_dict(stream, schema_path=FilePath(input_year).path)

    # Clean stream
    stream = cleaner.clean(stream)
    stream = logger.log_errors(stream)

    # Output results
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir, filename=filename)
    stream = csww_record.message_collector(stream)

    data_worker, data_lalevel = csww_record.export_table(stream)

    data_worker = file_creator.add_fields(input_year, data_worker, la_name)
    data_worker = file_creator.degrade_data(data_worker)
    file_creator.export_file(input, output, data_worker, "worker")

    data_lalevel = file_creator.add_fields(input_year, data_lalevel, la_name)
    file_creator.export_file(input, output, data_lalevel, "lalevel")


def la_agg(input, output):
    """
    Joins data from newly cleaned social work workforce census files (output of cleanfile()) to existing social work workforce census files for the depositing local authority
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
    if table_name == "CSWWWorker":
        dates = config["dates"]
        csww_df = agg_process.convert_datetimes(csww_df, dates, table_name)
    sort_order = config["sort_order"]
    dedup = config["dedup"]
    csww_df = agg_process.deduplicate(csww_df, table_name, sort_order, dedup)
    csww_df = agg_process.remove_old_data(
        csww_df,
        num_of_years=YEARS_TO_GO_BACK,
        new_year_start_month=YEAR_START_MONTH,
        as_at_date=REFERENCE_DATE,
    )

    # If file still has data, after removing old data: re-format and export merged file
    if len(csww_df) > 0:
        if table_name == "CSWWWorker":
            csww_df = agg_process.convert_dates(csww_df, dates, table_name)
        agg_process.export_la_file(output, table_name, csww_df)


def pan_agg(input, la_code, output):
    """
    Joins data from newly merged social work workforce file (output of la-agg()) to existing pan-London workforce data
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = pan_config.Config()

    # Read file and match type
    csww_df = pan_process.read_file(input)
    column_names = config["column_names"]
    table_name = pan_process.match_load_file(csww_df, column_names)

    # Remove unwanted datasets and merge wanted with existing output
    pan_data_kept = config["pan_data_kept"]
    if table_name in pan_data_kept:
        la_name = flip_dict(config["data_codes"])[la_code]
        csww_df = pan_process.merge_agg_files(output, table_name, csww_df, la_name)
        pan_process.export_pan_file(output, table_name, csww_df)


# Run in Visual Studio Code |>

# cleanfile(
#     "/workspaces/liia-tools/liiatools/spec/social_work_workforce/samples/csww/BAD/social_work_workforce_2022_sc.xml",
#     "BAD",
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
# )

# la_agg(
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean/social_work_workforce_2022_worker_clean.csv",
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
# )

# la_agg(
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean/social_work_workforce_2022_lalevel_clean.csv",
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
# )

# pan_agg(
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean/CSWW_CSWWWorker_merged.csv",
#     "BAD",
#     "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
# )
