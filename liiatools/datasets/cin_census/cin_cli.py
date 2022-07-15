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
    configuration,
    logger,
    validator,
    cin_record,
)
from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import flip_dict

# Dependencies for la_agg()
from liiatools.datasets.cin_census.lds_cin_la_agg import cc_la_agg

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
    """Cleans an input CIN Census xml file according to config and outputs a cleaned csv file.
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_name' should be a string of the name of the local authority that deposited the file
    'la_log_dir' should specify the path to the local authority's log folder
    'lds_log_dir' should specify the path to the LDS log folder
    'output' should specify the path to the output folder"""

    # Open & Parse file
    stream = dom_parse(input)

    # Configure stream
    config = configuration.Config()
    la_name = flip_dict(config["data_codes"])[la_code]
    stream = filters.strip_text(stream)
    stream = filters.add_context(stream)
    stream = filters.add_schema(stream, schema=Schema().schema)
    stream = logger.create_child_uuid(stream)
    map_dict = {}
    stream = logger.create_uuid_to_LAchildID_map(stream, map_dict=map_dict)
    stream = logger.map_uuid_to_LAchildID(stream, map_dict=map_dict)

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
@click.argument("input", type=click.Path(exists=True))
@click.argument("la_log_dir", type=click.Path(exists=True))
@click.argument("flat_output", type=click.Path(exists=True))
@click.argument("analysis_output", type=click.Path(exists=True))
def la_agg(input, la_log_dir, flat_output, analysis_output):
    """Aggregates all CIN Census files that have been uploaded by a local authority
    'input' should specify the input file location, including file name and suffix, and be usable by a Path function
    'la_log_dir' should specify the path to the local authority's log folder
    'flat_output' should specify the output directory for the flatfile output, and be usable by a Path function
    'analysis_output should specify the output directory for the analysis outputs, and be usable by a Path function"""

    # Create flat file
    flatfile = cc_la_agg.read_file(input)
    flatfile = cc_la_agg.merge_la_files(flat_output, flatfile, input)
    flatfile = cc_la_agg.deduplicate(flatfile, input)
    flatfile = cc_la_agg.remove_old_data(flatfile, input)
    flatfile = cc_la_agg.log_missing_years(flatfile, input, la_log_dir)
    cc_la_agg.export_flatfile(flat_output, flatfile, input)

    # Create fact file
    factors = cc_la_agg.factors_inputs(flatfile)
    factors = cc_la_agg.split_factors(factors)
    cc_la_agg.export_factfile(analysis_output, factors)

    # Create referral file
    ref, s17, s47 = cc_la_agg.referral_inputs(flatfile)
    ref_s17 = cc_la_agg.merge_ref_s17(ref, s17)
    ref_s47 = cc_la_agg.merge_ref_s47(ref, s47)
    ref_outs = cc_la_agg.ref_outcomes(ref, ref_s17, ref_s47)
    cc_la_agg.export_reffile(analysis_output, ref_outs)

    # Create journey file
    s47_outs = cc_la_agg.journey_inputs(flatfile)
    s47_journey = cc_la_agg.s47_paths(s47_outs)
    cc_la_agg.export_journeyfile(analysis_output, s47_journey)


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
