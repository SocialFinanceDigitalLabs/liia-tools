"""A set of functions that performs operations which convert input files into desired format with valid data."""

import os
from pathlib import Path
from typing import List, Dict, Final

import lxml.etree as etree
from pandas import DataFrame

import SWFtools.util.AppLogs as AppLog
import SWFtools.dataprocessing.validation.validator as validator
import SWFtools.dataprocessing.converter as converter
from SWFtools.util.work_path import la_directories, flatfile_folder

COLUMNS: Final[List[str]] = ["YearCensus", "LEA", "AgencyWorker", "SWENo", "FTE", "DoB_year", "Age", "GenderCurrent",
                             "Ethnicity", "QualInst", "QualLevel", "StepUpGrad", "OrgRole", "RoleStartDate", "StartOrigin",
                             "RoleEndDate", "LeaverDestination", "FTE30", "Cases30", "ContractWeeks", "FrontlineGrad",
                             "CFKSSstatus"]

COLUMNS_MERGED_FILE: Final[List[str]] = ["YearCensus", "LEA", "LEAName", "AgencyWorker", "SWENo", "FTE", "DoB_year", "Age",
                                         "GenderCurrent", "Gender", "Ethnicity", "Ethnicity_Group", "Ethnicity_Compact",
                                         "QualInst", "QualLevel", "QualLevelName", "StepUpGrad", "OrgRole", "OrgRoleName",
                                         "RoleStartDate", "StartOrigin", "RoleEndDate", "LeaverDestination", "FTE30",
                                         "Cases30", "ContractWeeks", "FrontlineGrad", "CFKSSstatus"]


def get_xml_files_from(directory: os.DirEntry) -> List[str]:
    """
    A function that returns a list of XML files that are directly inside the specified directory.
    :param directory: The directory that contains XML files. This is the LA directory
    :return: A list containing the names of the XML files. Each entry has the format: 'filename.xml'
    """
    directory_files = [file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]
    xml_files = [file for file in directory_files if file.endswith('.xml')]

    return xml_files


def parse_and_validate(la_directory: os.DirEntry, xml_file: str) -> List[Dict[str, str]] | None:
    """
    Parses an XML file, validates it and adds **'LEA'** and **'YearCensus'** common fields.
    :param la_directory: The directory that contains the XML file.
    :param xml_file: The name of the XML file. Format: 'filename.xml'
    :return: Returns a list of dictionaries containing valid worker data. If the file failed validation it returns None
    """
    # === PARSING DATA === #
    AppLog.log(f'Parsing \'{xml_file}\'...')
    parsed_xml = etree.parse(os.path.join(la_directory, xml_file))

    # === VALIDATING DATA === #
    validation_errors = []

    AppLog.log(f'Validating \'{xml_file}\'...',console_output=True)
    AppLog.log(f'Validating \'{xml_file}\'', la_directory.name)

    # Validation failed, continue with next file
    if not validator.is_acceptable(parsed_xml, validation_errors):
        message = f'The file \'{xml_file}\' failed validation:'
        AppLog.log(message, console_output=True)
        AppLog.log(message, la_directory.name)
        AppLog.log(validation_errors, la_directory.name)
        return None

    AppLog.log(f'The file \'{xml_file}\' passed minimum validation requirement', console_output=True)
    AppLog.log(f'Validating worker data...', console_output=True)

    # May contain 'None' values where worker data failed validation
    workers = [validator.get_valid_worker_data(worker, validation_errors) for worker in parsed_xml.iter('CSWWWorker')]

    # Validation summary data
    total_workers = len(workers)
    valid_count = sum(1 for worker in workers if worker is not None)
    validation_error_count = len(validation_errors)

    # Filter out the 'None' entries
    workers = [worker for worker in workers if worker is not None]

    # Print validation errors if there are any (worker validation)
    if validation_error_count != 0:
        AppLog.log('Worker validation errors: ', la_directory.name)
        AppLog.log(validation_errors, la_directory.name)

    # Print validation summary to LA and application log (runtime)
    messages = [f'{valid_count} out of {total_workers} worker records passed validation',
                f'{validation_error_count} worker validation errors.']
    AppLog.log(messages, la_directory.name)
    AppLog.log(messages, console_output=True)

    # === ADDING EXTENDED FIELDS === #
    # Pre-computed fields for conversion process
    lea = parsed_xml.xpath('//LEA')[0].text
    year_census = parsed_xml.xpath('//Year')[0].text

    converter.column_transfer(workers, lea, year_census)

    return workers


def process_all_input_files():
    """
    Validates XML files, processes worker data, and merges all validated XMLs.
    :return: None
    """
    AppLog.log_section_header('Processing all XML files inside CIN folder', console_output=True)

    processed_files_count = 0
    path_merged_file = os.path.join(flatfile_folder, 'merged_LA_files.csv')
    path_modified_merged_file = os.path.join(flatfile_folder, 'merged_modified.csv')

    for la_directory in la_directories:
        AppLog.log(f'Processing XML files inside: {la_directory.path}')

        xml_files = get_xml_files_from(la_directory)

        for xml_file in xml_files:

            workers = parse_and_validate(la_directory, xml_file)

            if workers is None:
                continue

            # === PROCESSING WORKER DATA === #
            AppLog.log('Processing worker data...', console_output=True)
            AppLog.log('Processing worker data...', la_directory.name)
            for worker in workers:
                converter.swe_hash(worker)
                converter.convert_dates(worker)

            # === WRITING TO CSV === #
            path_file = os.path.join(flatfile_folder, la_directory.name, Path(xml_file).stem + '.csv')
            data_frame = DataFrame(workers, columns=COLUMNS_MERGED_FILE)

            # Write single file
            data_frame.to_csv(path_file, index=False, columns=COLUMNS)

            # Write to common file (merged records file)
            # Either append data to existing file or write to file if it's the first file to be processed
            if processed_files_count != 0:
                # header = False (do not append column names again)
                # columns = COLUMNS | COLUMNS_MERGED_FILE (what columns/fields to write)
                # mode is the same as Python file modes ('w' write is default)
                data_frame.to_csv(path_merged_file, index=False, header=False, columns=COLUMNS, mode='a')
                data_frame.to_csv(path_modified_merged_file, index=False, header=False, columns=COLUMNS_MERGED_FILE, mode='a')
            else:
                data_frame.to_csv(path_merged_file, index=False, columns=COLUMNS)
                data_frame.to_csv(path_modified_merged_file, index=False, columns=COLUMNS_MERGED_FILE)

            processed_files_count = processed_files_count + 1

    AppLog.log(f'Finished processing {processed_files_count} files in {len(la_directories)} directories.',
               console_output=True)
