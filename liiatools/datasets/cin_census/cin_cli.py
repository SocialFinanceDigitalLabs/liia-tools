import logging
import click_log
import click as click
from pathlib import Path
import yaml


# Dependencies for cleanfile()
from sfdata_stream_parser.stream import events
from liiatools.csdatatools.util.xml import dom_parse
from liiatools.datasets.cin_census.lds_cin_clean.schema import Schema
from liiatools.csdatatools.datasets.cincensus import filters

from liiatools.datasets.cin_census.lds_cin_clean import (
    file_creator,
    configuration as clean_config,
    logger,
    validator,
    cin_record,
    converter,
)
from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import (
    flip_dict,
    check_file_type,
    supported_file_types,
)

# Dependencies for la_agg()
from liiatools.datasets.cin_census.lds_cin_la_agg import configuration as agg_config
from liiatools.datasets.cin_census.lds_cin_la_agg import process as agg_process

# Dependencies for pan_agg()
from liiatools.datasets.cin_census.lds_cin_pan_agg import configuration as pan_config
from liiatools.datasets.cin_census.lds_cin_pan_agg import process as pan_process

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
    blank_error = []
    stream = logger.counter(
        stream,
        counter_check=lambda e: isinstance(e, events.StartElement)
        and hasattr(e, "valid"),
        value_error=value_error,
        structural_error=structural_error,
        blank_error=blank_error,
    )

    # Clean stream
    stream = converter.convert_true_false(stream)
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
        blank_error=blank_error,
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
    help="A string specifying the directory location for the main flatfile output",
)
@click.option(
    "--analysis_output",
    required=True,
    type=str,
    help="A string specifying the directory location for the additional analysis outputs",
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
    flatfile = agg_process.remove_old_data(flatfile, years=6)

    # Output flatfile
    agg_process.export_flatfile(flat_output, flatfile)

    # Create and output factors file
    factors = agg_process.filter_flatfile(
        flatfile, filter="AssessmentAuthorisationDate"
    )
    if len(factors) > 0:
        factors = agg_process.split_factors(factors)
        agg_process.export_factfile(analysis_output, factors)

    # Create referral file
    ref, s17, s47 = agg_process.referral_inputs(flatfile)
    if len(s17) > 0 and len(s47) > 0:
        ref_assessment = config["ref_assessment"]
        ref_s17 = agg_process.merge_ref_s17(ref, s17, ref_assessment)
        ref_s47 = agg_process.merge_ref_s47(ref, s47, ref_assessment)
        ref_outs = agg_process.ref_outcomes(ref, ref_s17, ref_s47)
        agg_process.export_reffile(analysis_output, ref_outs)

    # Create journey file
    icpc_cpp_days = config["icpc_cpp_days"]
    s47_cpp_days = config["s47_cpp_days"]
    s47_j, cpp = agg_process.journey_inputs(flatfile)
    if len(s47_j) > 0 and len(cpp) > 0:
        s47_outs = agg_process.journey_merge(s47_j, cpp, icpc_cpp_days, s47_cpp_days)
        s47_day_limit = config["s47_day_limit"]
        icpc_day_limit = config["icpc_day_limit"]
        s47_journey = agg_process.s47_paths(s47_outs, s47_day_limit, icpc_day_limit)
        agg_process.export_journeyfile(analysis_output, s47_journey)


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
    "--flat_output",
    required=True,
    type=str,
    help="A string specifying the directory location for the main flatfile output",
)
@click.option(
    "--analysis_output",
    required=True,
    type=str,
    help="A string specifying the directory location for the additional analysis outputs",
)
def pan_agg(input, la_code, flat_output, analysis_output):
    """
    Joins data from newly merged CIN Census file (output of la-agg()) to existing pan-London CIN Census data and creates analytical outputs
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param flat_output: should specify the path to the folder for the main flatfile output
    :param analysis_output: should specify the path to the folder for the additional analytical outputs
    :return: None
    """

    # Configuration
    config = pan_config.Config()

    # Create flat file
    dates = config["dates"]
    flatfile = pan_process.read_file(input, dates)

    # Merge with existing pan-London data
    la_name = flip_dict(config["data_codes"])[la_code]
    flatfile = pan_process.merge_agg_files(flat_output, dates, la_name, flatfile)

    # Output flatfile
    pan_process.export_flatfile(flat_output, flatfile)

    # Create and output factors file
    factors = pan_process.filter_flatfile(
        flatfile, filter="AssessmentAuthorisationDate"
    )
    if len(factors) > 0:
        factors = pan_process.split_factors(factors)
        pan_process.export_factfile(analysis_output, factors)

    # Create referral file
    ref, s17, s47 = pan_process.referral_inputs(flatfile)
    if len(s17) > 0 and len(s47) > 0:
        ref_assessment = config["ref_assessment"]
        ref_s17 = pan_process.merge_ref_s17(ref, s17, ref_assessment)
        ref_s47 = pan_process.merge_ref_s47(ref, s47, ref_assessment)
        ref_outs = pan_process.ref_outcomes(ref, ref_s17, ref_s47)
        pan_process.export_reffile(analysis_output, ref_outs)

    # Create journey file
    icpc_cpp_days = config["icpc_cpp_days"]
    s47_cpp_days = config["s47_cpp_days"]
    s47_j, cpp = pan_process.journey_inputs(flatfile)
    if len(s47_j) > 0 and len(cpp) > 0:
        s47_outs = pan_process.journey_merge(s47_j, cpp, icpc_cpp_days, s47_cpp_days)
        s47_day_limit = config["s47_day_limit"]
        icpc_day_limit = config["icpc_day_limit"]
        s47_journey = pan_process.s47_paths(s47_outs, s47_day_limit, icpc_day_limit)
        pan_process.export_journeyfile(analysis_output, s47_journey)
