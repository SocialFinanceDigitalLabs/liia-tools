from pathlib import Path
import pandas as pd
import logging

from liiatools.datasets.shared_functions import converters, common

log = logging.getLogger(__name__)


def convert_to_dataframe(data):
    data = data.export("df")
    return data


def get_year(data, year):
    data["YEAR"] = year
    return data


# def convert_to_datetime(data):
#     data[["PersonBirthDate", "RoleStartDate"]] = data[
#         ["PersonBirthDate", "RoleStartDate"]
#     ].apply(pd.to_datetime)
#     return data

def convert_to_datetime(data):
    if set(["PersonBirthDate", "RoleStartDate"]).issubset(data):
        data[["PersonBirthDate", "RoleStartDate"]] = data[
            ["PersonBirthDate", "RoleStartDate"]
        ].apply(pd.to_datetime)
    return data


def add_la_name(data, la_name):
    data["LA"] = la_name
    return data

def degrade_dob(data):
    if "PersonBirthDate" in data:
        if data["PersonBirthDate"] is not None:
            data["PersonBirthDate"] = data["PersonBirthDate"].apply(
                lambda row: converters.to_month_only_dob(row)
            )
    return data


# def la_prefix(data, la_code):
#     data["SWENo"] = data["SWENo"] + "_" + la_code
#     return data

def la_prefix(data, la_code):
    if set(["SWENo"]).issubset(data):
        data["SWENo"] = data["SWENo"] + "_" + la_code
        return data


def add_fields(input_year, data, la_name, la_code):
    """
    Add YEAR, LA, PERSONSCHOOLYEAR to exported dataframe
    Append LA_code from config to LAChildID

    :param input_year: A string of the year of return for the current file
    :param data: The dataframe to be cleaned
    :param la_name: LA name
    :param la_code: LA code
    :return: Cleaned and degraded dataframe
    """
    data = convert_to_dataframe(data)
    data = get_year(data, input_year)
    data = convert_to_datetime(data)

    data = add_la_name(data, la_name)
    data = la_prefix(data, la_code)
    data = degrade_dob(data)

    return data


def export_file(input, output, data, filenamelevel):
    filename = Path(input).stem
    outfile = filename + "_" + filenamelevel + "_clean.csv"
    output_path = Path(output, outfile)
    data.to_csv(output_path, index=False)
