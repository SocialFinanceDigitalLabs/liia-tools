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


def convert_to_datetime(data):
    data[["PersonBirthDate", "RoleStartDate"]] = data[
        ["PersonBirthDate", "RoleStartDate"]
    ].apply(pd.to_datetime)
    return data


def add_la_name(data, la_name):
    data["LA"] = la_name
    return data


# def la_prefix(data, la_code):
#     data["LAchildID"] = data["LAchildID"] + "_" + la_code
#     return data


def add_fields(input_year, data, la_name):
    """
    Add YEAR, LA to exported dataframe

    :param input_year: A string of the year of return for the current file
    :param data: The dataframe to be cleaned
    :param la_name: LA name
    :return: Cleaned and degraded dataframe
    """
    data = convert_to_dataframe(data)
    data = get_year(data, input_year)
    data = add_la_name(data, la_name)

    # data = convert_to_datetime(data)
    # data = la_prefix(data, la_code)
    # data = degrade_dob(data)
    # data = degrade_expected_dob(data)
    # data = degrade_death_date(data)
    return data


def export_file(input, output, data, filenamelevel):
    """
    Output cleansed and degraded dataframe as csv file.
    Example of output filename: social_work_workforce_2022_lalevel_clean.csv

    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param output: should specify the path to the output folder
    :param data: The cleansed dataframe to be output
    :param filenamelevel: String appended to output filename indicating aggregation level - worker or LA level
    :return: csv file containing cleaned and degraded dataframe
    """
    filenamestem = Path(input).stem
    outfile = filenamestem + "_" + filenamelevel + "_clean.csv"
    output_path = Path(output, outfile)
    data.to_csv(output_path, index=False)
