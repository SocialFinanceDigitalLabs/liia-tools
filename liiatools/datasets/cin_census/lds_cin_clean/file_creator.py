from pathlib import Path
import re
import pandas as pd

from liiatools.datasets.cin_census.lds_cin_clean import la_codes
from liiatools.datasets.shared_functions import converters


def convert_to_dataframe(data):
    data = data.export("df")
    return data


def get_year(input, data):
    filename = Path(input).stem
    match = re.search(r"20\d{2}", filename)
    year = match.group(0)
    data["YEAR"] = year
    return data


def convert_to_datetime(data):
    data[["PersonBirthDate", "PersonSchoolYear"]] = data[
        ["PersonBirthDate", "PersonSchoolYear"]
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


def la_prefix(data, la_name):
    la_prefix = la_codes.la_codes[la_name]
    data["LAchildID"] = la_prefix + "_" + data["LAchildID"]
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


def add_fields(input, data, la_name):
    """Adds YEAR, LA, PERSONSCHOOLYEAR to exported dataframe
    Appends LA_code from config to LAChildID"""
    data = convert_to_dataframe(data)
    data = get_year(input, data)
    data = convert_to_datetime(data)
    data = add_school_year(data)
    data = add_la_name(data, la_name)
    data = la_prefix(data, la_name)
    data = degrade_dob(data)
    data = degrade_expected_dob(data)
    data = degrade_death_date(data)
    return data


def export_file(input, output, data):
    filename = Path(input).stem
    outfile = filename + "_clean.csv"
    output_path = Path(output, outfile)
    data.to_csv(output_path, index=False)
