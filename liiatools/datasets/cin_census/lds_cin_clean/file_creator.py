from pathlib import Path
import pandas as pd
import logging

from liiatools.datasets.shared_functions import converters, common

log = logging.getLogger(__name__)


def convert_to_dataframe(data):
    data = data.export("df")
    return data


def get_year(input, data, la_log_dir):
    filename = Path(input).stem
    try:
        year = common.check_year(filename)
        data["YEAR"] = year
        return data
    except AttributeError:
        common.save_year_error(input, la_log_dir)
        raise Exception(
            f"{filename} was not processed as the filename did not contain a year, "
            f"this error has been sent to the LA to be fixed"
        )


def convert_to_datetime(data):
    data[["PersonBirthDate", "PersonDeathDate", "ExpectedPersonBirthDate"]] = data[
        ["PersonBirthDate", "PersonDeathDate", "ExpectedPersonBirthDate"]
    ].apply(pd.to_datetime)
    return data


def _get_person_school_year(datevalue):
    if datevalue.month >= 9:
        school_year = datevalue.year
    elif datevalue.month <= 8:
        school_year = datevalue.year - 1
    else:
        school_year = None
    return school_year


def add_school_year(data):
    data["PersonSchoolYear"] = data["PersonBirthDate"].apply(
        lambda row: _get_person_school_year(row)
    )
    return data


def add_la_name(data, la_name):
    data["LA"] = la_name
    return data


def la_prefix(data, la_code):
    data["LAchildID"] = data["LAchildID"] + "_" + la_code
    return data


def degrade_dob(data):
    if data["PersonBirthDate"] is not None:
        data["PersonBirthDate"] = data["PersonBirthDate"].apply(
            lambda row: converters.to_month_only_dob(row)
        )
        return data


def degrade_expected_dob(data):
    if data["ExpectedPersonBirthDate"] is not None:
        data["ExpectedPersonBirthDate"] = data["ExpectedPersonBirthDate"].apply(
            lambda row: converters.to_month_only_dob(row)
        )
        return data


def degrade_death_date(data):
    if data["PersonDeathDate"] is not None:
        data["PersonDeathDate"] = data["PersonDeathDate"].apply(
            lambda row: converters.to_month_only_dob(row)
        )
        return data


def add_fields(input, data, la_name, la_log_dir, la_code):
    """Adds YEAR, LA, PERSONSCHOOLYEAR to exported dataframe
    Appends LA_code from config to LAChildID"""
    data = convert_to_dataframe(data)
    data = get_year(input, data, la_log_dir)
    data = convert_to_datetime(data)
    data = add_school_year(data)
    data = add_la_name(data, la_name)
    data = la_prefix(data, la_code)
    data = degrade_dob(data)
    data = degrade_expected_dob(data)
    data = degrade_death_date(data)
    return data


def export_file(input, output, data):
    filename = Path(input).stem
    outfile = filename + "_clean.csv"
    output_path = Path(output, outfile)
    data.to_csv(output_path, index=False)
