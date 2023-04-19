"""Functions for validating XML files of LAs and getting validated worker data"""

from typing import List, Dict, Final

import lxml.etree as etree

import xmlschema
from xmlschema import XMLSchemaValidationError
from SWFtools.util.work_path import XML_SCHEMA

from SWFtools.dataprocessing.validation.validation_error import ValidationError, ERROR_CAUSE

CIN_XML_SCHEMA: Final = xmlschema.XMLSchema11(XML_SCHEMA)
WORKER_SCHEMA: Final = CIN_XML_SCHEMA.find('//CSWWWorker')
NON_AGENCY_MANDATORY_TAG: Final = ['PersonBirthDate', 'GenderCurrent', 'Ethnicity', 'QualInst', 'StepUpGrad',
                                    'OrgRole', 'RoleStartDate', 'StartOrigin', 'FTE30', 'WorkingDaysLost', 'FrontlineGrad']


def convert_schema_error(schema_error: XMLSchemaValidationError) -> ValidationError:
    """
    Converts an **XMLSchemaValidationError** to a custom validation error. See *'validation_error.py'*
    """

    # Structural error
    # For this error view source file xmlschema/validators/groups.py (at line 1266)
    if schema_error.validator.__class__ == xmlschema.validators.Xsd11Group:
        # tuple (minOccurs, maxOccurs) - these numbers are specified inside the XML schema
        occurs_range = schema_error.args[3].occurs

        # An <int> representing how many times the tag that is being validated occurs
        # This value is 0 if there is a missmatch between the tag that was expected and the
        # tag that was actually read

        occurs = schema_error.args[4]

        # The tag of the read element
        read_tag = schema_error.elem[schema_error.args[2]].tag

        # The tag of the expected element
        expected_tag = schema_error.args[3].name

        if read_tag != expected_tag:
            return ValidationError(ERROR_CAUSE.GROUP_FORMAT, schema_error.elem.tag, schema_error.sourceline)
        elif occurs < occurs_range[0]:
            return ValidationError(ERROR_CAUSE.MISSING_TAG, schema_error.elem.tag, schema_error.sourceline)
        elif occurs > occurs_range[1]:
            return ValidationError(ERROR_CAUSE.TOO_MANY_TAGS, schema_error.elem.tag, schema_error.sourceline)
        else:
            print(f'UNEXPECTED ERROR!\n{schema_error}')
            return None
    # Value does not match to any enumerated value
    elif schema_error.validator.__class__ == xmlschema.validators.XsdEnumerationFacets:
        return ValidationError(ERROR_CAUSE.VALUE, schema_error.elem.tag, schema_error.sourceline)
    # Value does not match pattern specified
    elif schema_error.validator.__class__ == xmlschema.validators.XsdPatternFacets:
        return ValidationError(ERROR_CAUSE.VALUE, schema_error.elem.tag, schema_error.sourceline)
    # Value is of unexpected type
    elif schema_error.validator.__class__ == xmlschema.validators.XsdAtomicBuiltin:
        return ValidationError(ERROR_CAUSE.TYPE, schema_error.elem.tag, schema_error.sourceline)
    # Value is below accepted range minimum inclusive
    elif schema_error.validator.__class__ == xmlschema.validators.XsdMinInclusiveFacet:
        return ValidationError(ERROR_CAUSE.MIN_RANGE, schema_error.elem.tag, schema_error.sourceline)
    # Value is above accepted range maximum inclusive
    elif schema_error.validator.__class__ == xmlschema.validators.XsdMaxInclusiveFacet:
        return ValidationError(ERROR_CAUSE.MAX_RANGE, schema_error.elem.tag, schema_error.sourceline)


def is_acceptable(parsed_xml: etree.Element, error_list: List[ValidationError]) -> bool:
    """
    Validates **'Header'** and **'LALevelVacancies'** tags then checks that the format of the message is correct:
    **'Header'**, **'LALevelVacancies'** followed by **'CSWWWorker'** n times

    :param parsed_xml: The root element of the xml document to be checked.
    :param error_list: A list where all validation errors are placed into.
    :return: Returns 'True' if the document can be further processed of 'False' otherwise.
    """

    if not __is_message_format_valid(parsed_xml, error_list):
        return False

    la_vacancies_schema = CIN_XML_SCHEMA.find('//LALevelVacancies')
    la_vacancies_xml = parsed_xml.xpath('//LALevelVacancies')[0]

    header_schema = CIN_XML_SCHEMA.find('//Header')
    header_xml = parsed_xml.xpath('//Header')[0]

    verr = [convert_schema_error(error) for error in header_schema.iter_errors(header_xml)]
    verr.extend([convert_schema_error(error) for error in la_vacancies_schema.iter_errors(la_vacancies_xml)])

    if len(verr) != 0:
        error_list.extend(verr)
        return False

    return True


# Shallow validation of 'Header', 'LALevelVacancies', and 'CSWWWorker' tags
def __is_message_format_valid(parsed_xml: etree.ElementTree, error_list: List[ValidationError]) -> bool:
    can_continue = True

    for error in CIN_XML_SCHEMA.iter_errors(parsed_xml, max_depth=1):
        va = convert_schema_error(error)

        # A group format means that the Header, LAVacancies and CSWWWorker tags are just out of order, can still proceed
        if va is not None and va.cause != ERROR_CAUSE.GROUP_FORMAT:
            can_continue = False

        error_list.append(va)

    return can_continue


def get_valid_worker_data(worker_element: etree.Element, error_list: List[ValidationError]) -> Dict[str, str]:
    """
    Takes in a worker element (lxml.etree), validates it and returns the worker data as a dictionary
    or **'None'** if the worker data is invalid.

    :param worker_element: A tree element (lxml.etree) that has the tag 'CSWWWorker' .
    :param error_list: A list where all validation errors are placed into.
    :return: A dictionary that contains the worker data. Child tags are the keys and
    their value as the dictionary's value
    """

    # === XML SCHEMA VALIDATION ===
    va: List[ValidationError] = list(map(convert_schema_error, WORKER_SCHEMA.iter_errors(worker_element)))

    # === BUILDING WORKER DATA SET; FURTHER VALIDATION ===
    worker_data = {}

    for elem in worker_element:
        worker_data[elem.tag] = elem.text

    is_agency_worker = worker_data['AgencyWorker'] == '1'
    is_leaver = 'RoleEndDate' in worker_data
    is_starter = 'RoleStartDate' in worker_data

    # Validation rules if the worker is a non-agency worker
    if not is_agency_worker:
        # If a worker is a non-agency worker, check that mandatory tags are present
        for tag in NON_AGENCY_MANDATORY_TAG:
            if tag not in worker_data:
                va.append(ValidationError(ERROR_CAUSE.NON_AGENCY_MANDATORY_MISSING, tag, worker_element.sourceline))

    # Validation rules if the worker is a starter (condition checked via eXclusive OR)
    if is_starter ^ ('StartOrigin' in worker_data):
        va.append(ValidationError(ERROR_CAUSE.MISSING_TAG, 'StartOrigin', worker_element.sourceline))

    # Validation rules if the worker is a leaver
    if is_leaver and worker_data['FTE'] != '0':
        sourceline = worker_element.xpath('//FTE')[0].sourceline
        va.append(ValidationError(ERROR_CAUSE.LEAVER_FTE, 'FTE', sourceline))

    if is_leaver ^ ('ReasonLeave' in worker_data):
        if is_leaver:
            va.append(ERROR_CAUSE.MISSING_TAG, 'ReasonLeave', worker_element.sourceline)
        else:
            va.append(ValidationError(ERROR_CAUSE.LEAVER_UNEXPECTED, 'ReasonLeave', worker_element.sourceline))

    if is_leaver ^ ('LeaverDestination' in worker_data):
        if is_leaver:
            va.append(ERROR_CAUSE.MISSING_TAG, 'LeaverDestination', worker_element.sourceline)
        else:
            va.append(ValidationError(ERROR_CAUSE.LEAVER_UNEXPECTED, 'LeaverDestination', worker_element.sourceline))

    # If there are any errors the element is invalid, return None and move on
    if len(va) != 0:
        error_list.extend(va)
        return None

    return worker_data
