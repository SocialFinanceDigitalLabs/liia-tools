import logging
import click_log
import click as click
from pathlib import Path
import yaml


# Dependencies for cleanfile()
from sfdata_stream_parser.stream import events

from csdatatools.util.xml import dom_parse
from csdatatools.datasets.cincensus.schema import Schema
from csdatatools.datasets.cincensus import filters

from liiatools.datasets.cin_census.lds_cin_clean import (
    file_creator,
    configuration as clean_config,
    logger,
    validator,
    cin_record,
)
from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import flip_dict

# Dependencies for la_agg()
from liiatools.datasets.cin_census.lds_cin_la_agg import configuration as agg_config
from liiatools.datasets.cin_census.lds_cin_la_agg import process as agg_process

# Dependencies for pan_agg()
from liiatools.datasets.cin_census.lds_cin_pan_agg import cc_pan_agg

log = logging.getLogger()
click_log.basic_config(log)

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())


@click.group()
def cin_census():
    """Functions for cleaning, minimising and aggregating CIN Census files"""
    pass


@cin_census.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--la_code",
    required=True,
    type=click.Choice(la_list, case_sensitive=False),
    help="A three letter code, specifying the local authority that deposited the file",
)
@click.option(
    "--la_log_dir",
    required=True,
    type=str,
    help="A string specifying the location that the log files for the LA should be output, usable by a pathlib Path function.",
)
@click.option(
    "--o",
    "output",
    required=True,
    type=str,
    help="A string specifying the output directory location",
)
@click_log.simple_verbosity_option(log)
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
    stream = dom_parse(input)

    # Configure stream
    config = clean_config.Config()
    la_name = flip_dict(config["data_codes"])[la_code]
    stream = filters.strip_text(stream)
    stream = filters.add_context(stream)
    stream = filters.add_schema(stream, schema=Schema().schema)
    stream = logger.inherit_LAchildID(stream)

    # Validate stream
    field_error = []
    LAchildID_error = []
    stream = validator.validate_elements(
        stream, LAchildID_error=LAchildID_error, field_error=field_error
    )
    value_error = []
    structural_error = []
    stream = logger.counter(
        stream,
        counter_check=lambda e: isinstance(e, events.StartElement)
        and hasattr(e, "valid"),
        value_error=value_error,
        structural_error=structural_error,
        LAchildID_error=LAchildID_error,
    )

    # Clean stream
    tags = [
        "LAchildID",
        "UPN",
        "FormerUPN",
        "UPNunknown",
        "PersonBirthDate",
        "GenderCurrent",
        "PersonDeathDate",
        "Ethnicity",
        "Disability",
        "CINreferralDate",
        "ReferralSource",
        "PrimaryNeedCode",
        "CINclosureDate",
        "ReasonForClosure",
        "DateOfInitialCPC",
        "AssessmentActualStartDate",
        "AssessmentInternalReviewDate",
        "AssessmentAuthorisationDate",
        "AssessmentFactors",
        "CINPlanStartDate",
        "CINPlanEndDate",
        "S47ActualStartDate",
        "InitialCPCtarget",
        "DateOfInitialCPC",
        "ICPCnotRequired",
        "ReferralNFA",
        "CPPstartDate",
        "CPPendDate",
        "InitialCategoryOfAbuse",
        "LatestCategoryOfAbuse",
        "NumberOfPreviousCPP",
        "CPPreviewDate",
    ]
    stream = validator.remove_invalid(stream, tag_list=tags)

    # Output result
    stream = cin_record.message_collector(stream)
    data = cin_record.export_table(stream)
    data = file_creator.add_fields(input, data, la_name, la_log_dir, la_code)
    file_creator.export_file(input, output, data)
    logger.save_errors_la(
        input,
        value_error=value_error,
        structural_error=structural_error,
        LAchildID_error=LAchildID_error,
        field_error=field_error,
        la_log_dir=la_log_dir,
    )


@cin_census.command()
@click.option(
    "--i",
    "input",
    required=True,
    type=str,
    help="A string specifying the input file location, including the file name and suffix, usable by a pathlib Path function",
)
@click.option(
    "--flat_output",
    required=True,
    type=str,
    help="A string specifying the directory location for the main flatfile output"
)
@click.option(
    "--analysis_output",
    required=True,
    type=str,
    help="A string specifying the directory location for the additional analysis outputs"
)
def la_agg(input, flat_output, analysis_output):
    """
    Joins data from newly cleaned CIN Census file (output of cleanfile()) to existing CIN Census data for the depositing local authority
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param flat_output: should specify the path to the folder for the main flatfile output
    :param analysis_output: should specify the path to the folder for the additional analytical outputs
    :return: None
    """

    # Configuration
    config = agg_config.Config()

    # Open file as Dataframe
    dates = config["dates"]
    flatfile = agg_process.read_file(input, dates)

    # Merge with existing data, de-duplicate and apply data retention policy
    flatfile = agg_process.merge_la_files(flat_output, dates, flatfile)
    sort_order = config["sort_order"]
    dedup = config["dedup"]
    flatfile = agg_process.deduplicate(flatfile, sort_order, dedup)
    flatfile = agg_process.remove_old_data(flatfile, years = 6)

    # Output flatfile
    agg_process.export_flatfile(flat_output, flatfile)

    # Create and output factors file
    factors = agg_process.filter_flatfile(flatfile, filter="AssessmentAuthorisationDate")
    factors = agg_process.split_factors(factors)
    agg_process.export_factfile(analysis_output, factors)

    # Create referral file
    ref, s17, s47 = agg_process.referral_inputs(flatfile)
    ref_assessment = config["ref_assessment"]
    ref_s17 = agg_process.merge_ref_s17(ref, s17, ref_assessment)
    ref_s47 = agg_process.merge_ref_s47(ref, s47, ref_assessment)
    ref_outs = agg_process.ref_outcomes(ref, ref_s17, ref_s47)
    agg_process.export_reffile(analysis_output, ref_outs)

    # Create journey file
    icpc_cpp_days = config["icpc_cpp_days"]
    s47_cpp_days = config["s47_cpp_days"]
    s47_outs = agg_process.journey_inputs(flatfile, icpc_cpp_days, s47_cpp_days)
    s47_day_limit = config["s47_day_limit"]
    icpc_day_limit = config["icpc_day_limit"]
    s47_journey = agg_process.s47_paths(s47_outs, s47_day_limit, icpc_day_limit)
    agg_process.export_journeyfile(analysis_output, s47_journey)


@cin_census.command()
@click.argument("input", type=click.Path(exists=True))
@click.argument("la_name", type=str)
@click.argument("flat_output", type=click.Path(exists=True))
@click.argument("analysis_output", type=click.Path(exists=True))
def pan_agg(input, la_name, flat_output, analysis_output):
    """Adds new data from one local authority to the existing pan-London aggregated files
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority whose data is being aggregated
    'flat_output' should specify the output directory for the flatfile output, and be usable by a Path function
    'analysis_output should specify the output directory for the analysis outputs, and be usable by a Path function"""

    # Create flat file
    flatfile = cc_pan_agg.read_file(input)
    flatfile = cc_pan_agg.merge_agg_files(flat_output, la_name, flatfile)
    cc_pan_agg.export_flatfile(flat_output, flatfile)

    # Create fact file
    factors = cc_pan_agg.factors_inputs(flatfile)
    factors = cc_pan_agg.split_factors(factors)
    cc_pan_agg.export_factfile(analysis_output, factors)

    # Create referral file
    ref, s17, s47 = cc_pan_agg.referral_inputs(flatfile)
    ref_s17 = cc_pan_agg.merge_ref_s17(ref, s17)
    ref_s47 = cc_pan_agg.merge_ref_s47(ref, s47)
    ref_outs = cc_pan_agg.ref_outcomes(ref, ref_s17, ref_s47)
    cc_pan_agg.export_reffile(analysis_output, ref_outs)

    # Create journey file
    s47_outs = cc_pan_agg.journey_inputs(flatfile)
    s47_journey = cc_pan_agg.s47_paths(s47_outs)
    cc_pan_agg.export_journeyfile(analysis_output, s47_journey)

# input = r"C:\Users\Michael.Hanks\OneDrive - Social Finance Ltd\Desktop\Fake LDS 2\LDS folders\Bromley\CIN Census\CIN_Census_2021_factors_clean.csv"
# config = agg_config.Config()
# dates = config["dates"]
# flatfile = agg_process.read_file(input, dates)
# ref, s17, s47 = agg_process.referral_inputs(flatfile)
# ref_assessment = config["ref_assessment"]
# ref_s17 = agg_process.merge_ref_s17(ref, s17, ref_assessment)