import logging
import click_log
import click as click
from pathlib import Path
import yaml
from datetime import datetime

from liiatools.datasets.social_work_workforce.sample_data import (
    generate_sample_csww_file,
)
from liiatools.csdatatools.util.stream import consume
from liiatools.csdatatools.util.xml import  etree, to_xml






# Dependencies for cleanfile()
#from sfdata_stream_parser.stream import events
from liiatools.csdatatools.util.xml import  dom_parse
from liiatools.datasets.social_work_workforce.lds_csww_clean.schema import Schema

# liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean/schema.py

from liiatools.csdatatools.datasets.cincensus import filters

from liiatools.datasets.social_work_workforce.lds_csww_clean import (
    csww_record,
    file_creator,
    configuration as clean_config,
    #validator,
    #converter,
    #logger,
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

log = logging.getLogger()
click_log.basic_config(log)

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
   la_list = list(yaml.full_load(las)["data_codes"].values())




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
        
@click.group()
def cin_census():
    """Functions for cleaning, minimising and aggregating CIN Census files"""
    pass


#@cin_census.command()
#@click.option(
 #   "--i",
#    "input",
 #   required=True,
#    type=str,
 #   help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
#)
#@click.option(
#    "--la_code",
 #   required=True,
#    type=click.Choice(la_list, case_sensitive=False),
#    help="A three letter code, specifying the local authority that deposited the file",
#)
#@click.option(
#    "--la_log_dir",
#    required=True,
 #   type=str,
#    help="A string specifying the location that the log files for the LA should be output, usable by a pathlib Path function.",
#)
#@click.option(
 #   "--o",
#    "output",
#    required=True,
#    type=str,
#    help="A string specifying the output directory location",
#)
@click_log.simple_verbosity_option(log)        
        
def cleanfile(input,la_code, la_log_dir,output):
    """
    Cleans input CIN Census xml files according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """
   #  return output
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
    if check_year_within_range(input_year, years_to_go_back, year_start_month, reference_date) is False:
        save_incorrect_year_error(input, la_log_dir)
        return

# Configure stream
    config = clean_config.Config()
    la_name = flip_dict(config["data_codes"])[la_code]
    stream = filters.strip_text(stream)
    stream = filters.add_context(stream)
    stream = filters.add_schema(stream, schema=Schema(input_year).schema)
    #stream = logger.inherit_LAchildID(stream)
    # Clean stream

# Output result
    stream = csww_record.message_collector(stream)
    data = csww_record.export_table(stream)
    data = file_creator.add_fields(input_year, data, la_name)
    file_creator.export_file(input, output, data)
#     log.save_errors_la(
#         input,
#         value_error=value_error,
#         structural_error=structural_error,
#         #LAchildID_error=LAchildID_error,
#         field_error=field_error,
#         blank_error=blank_error,
#         la_log_dir=la_log_dir,
#    )
    for e in stream :
        print (e.as_dict())
    list (stream)
cleanfile(
    "/workspaces/liia-tools/liiatools/spec/social_work_workforce/samples/csww/BAD/social_work_workforce_2022.xml",
            "BAD",
           "/workspaces/liia_tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
            "/workspaces/liia-tools/liiatools/datasets/social_work_workforce/lds_csww_clean",
           )  
print("===> Finished running csww_main_functions.py")
