"""A set of functions that performs operations which convert input files into desired format with valid data."""

import os
from pathlib import Path
from typing import List, Dict, Final

import lxml.etree as etree
from pandas import DataFrame

import liiatools.datasets.social_work_workforce.SWFtools.util.AppLogs as AppLog
import liiatools.datasets.social_work_workforce.SWFtools.dataprocessing.validation.validator as validator
import liiatools.datasets.social_work_workforce.SWFtools.dataprocessing.converter as converter
from liiatools.datasets.social_work_workforce.SWFtools.util.work_path import (
    la_directories,
    flatfile_folder,
)
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

COLUMNS: Final[List[str]] = [
    "YearCensus",
    "LEA",
    "AgencyWorker",
    "SWENo",
    "FTE",
    "DoB_year",
    "Age",
    "GenderCurrent",
    "Ethnicity",
    "QualInst",
    "QualLevel",
    "StepUpGrad",
    "OrgRole",
    "RoleStartDate",
    "StartOrigin",
    "RoleEndDate",
    "LeaverDestination",
    "FTE30",
    "Cases30",
    "ContractWeeks",
    "FrontlineGrad",
    "CFKSSstatus",
]

COLUMNS_MERGED_FILE: Final[List[str]] = [
    "YearCensus",
    "LEA",
    "LEAName",
    "AgencyWorker",
    "SWENo",
    "FTE",
    "DoB_year",
    "Age",
    "GenderCurrent",
    "Gender",
    "Ethnicity",
    "Ethnicity_Group",
    "Ethnicity_Compact",
    "QualInst",
    "QualLevel",
    "QualLevelName",
    "StepUpGrad",
    "OrgRole",
    "OrgRoleName",
    "RoleStartDate",
    "StartOrigin",
    "RoleEndDate",
    "LeaverDestination",
    "FTE30",
    "Cases30",
    "ContractWeeks",
    "FrontlineGrad",
    "CFKSSstatus",
]


def get_xml_files_from(directory: os.DirEntry) -> List[str]:
    """
    A function that returns a list of XML files that are directly inside the specified directory.
    :param directory: The directory that contains XML files. This is the LA directory
    :return: A list containing the names of the XML files. Each entry has the format: 'filename.xml'
    """
    directory_files = [
        file
        for file in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, file))
    ]
    xml_files = [file for file in directory_files if file.endswith(".xml")]

    return xml_files


def parse_and_validate(
    la_directory: os.DirEntry, xml_file: str
) -> List[Dict[str, str]] | None:
    """
    Parses an XML file, validates it and adds **'LEA'** and **'YearCensus'** common fields.
    :param la_directory: The directory that contains the XML file.
    :param xml_file: The name of the XML file. Format: 'filename.xml'
    :return: Returns a list of dictionaries containing valid worker data. If the file failed validation it returns None
    """
    # === PARSING DATA === #
    AppLog.log(f"Parsing '{xml_file}'...")
    parsed_xml = etree.parse(os.path.join(la_directory, xml_file))

    # === VALIDATING DATA === #
    validation_errors = []

    AppLog.log(f"Validating '{xml_file}'...", console_output=True)
    AppLog.log(f"Validating '{xml_file}'", la_directory.name)

    # Validation failed, continue with next file
    if not validator.is_acceptable(parsed_xml, validation_errors):
        message = f"The file '{xml_file}' failed validation:"
        AppLog.log(message, console_output=True)
        AppLog.log(message, la_directory.name)
        AppLog.log(validation_errors, la_directory.name)
        return None

    AppLog.log(
        f"The file '{xml_file}' passed minimum validation requirement",
        console_output=True,
    )
    AppLog.log(f"Validating worker data...", console_output=True)

    # May contain 'None' values where worker data failed validation
    workers = [
        validator.get_valid_worker_data(worker, validation_errors)
        for worker in parsed_xml.iter("CSWWWorker")
    ]

    # Validation summary data
    total_workers = len(workers)
    valid_count = sum(1 for worker in workers if worker is not None)
    validation_error_count = len(validation_errors)

    # Filter out the 'None' entries
    workers = [worker for worker in workers if worker is not None]

    # Print validation errors if there are any (worker validation)
    if validation_error_count != 0:
        AppLog.log("Worker validation errors: ", la_directory.name)
        AppLog.log(validation_errors, la_directory.name)

    # Print validation summary to LA and application log (runtime)
    messages = [
        f"{valid_count} out of {total_workers} worker records passed validation",
        f"{validation_error_count} worker validation errors.",
    ]
    AppLog.log(messages, la_directory.name)
    AppLog.log(messages, console_output=True)

    # === ADDING EXTENDED FIELDS === #
    # Pre-computed fields for conversion process
    lea = parsed_xml.xpath("//LEA")[0].text
    year_census = parsed_xml.xpath("//Year")[0].text

    converter.column_transfer(workers, lea, year_census)

    return workers


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
