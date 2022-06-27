from pathlib import Path
import re

from liiatools.datasets.cin_census.lds_cin_clean import la_codes
from liiatools.datasets.shared_functions import converters


def get_year(input, data):
    filename = Path(input).stem
    match = re.search(r"20\d{2}", filename)
    year = match.group(0)
    data["YEAR"] = year
    return data


def get_person_school_year(data):
    if data["PersonBirthDate"].dt.month >= 9:
        data["PersonSchoolYear"] = data["PersonBirthDate"].dt.year
    elif data["PersonBirthDate"].dt.month <= 8:
        data["PersonSchoolYear"] = data["PersonBirthDate"].dt.year - 1
    else:
        data["PersonSchoolYear"] = None
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
        data["PersonBirthDate"] = converters.to_month_only_dob(data["PersonBirthDate"])
    return data


def degrade_expected_dob(data):
    if data["ExpectedPersonBirthDate"] is not None:
        data["ExpectedPersonBirthDate"] = converters.to_month_only_dob(
            data["ExpectedPersonBirthDate"]
        )
    return data


def degrade_death_date(data):
    if data["PersonDeathDate"] is not None:
        data["PersonDeathDate"] = converters.to_month_only_dob(data["PersonDeathDate"])
    return data


def add_fields(input, data, la_name):
    """Adds YEAR, LA, PERSONSCHOOLYEAR to exported dataframe
    Appends LA_code from config to LAChildID"""

    data = get_year(input, data)
    data = get_person_school_year(data)
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
