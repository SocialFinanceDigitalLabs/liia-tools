from pathlib import Path
import pandas as pd
import logging
import hashlib
from typing import Dict
from decouple import config

from liiatools.datasets.shared_functions import converters, common

log = logging.getLogger(__name__)


def convert_to_dataframe(data):
    data = data.export("df")
    return data


def add_year(data, year):
    data["YEAR"] = year
    return data


def convert_to_datetime(data):
    if {"PersonBirthDate", "RoleStartDate"}.issubset(data):
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


def degrade_SWENo(data):
    """
    Replaces SWE number with hashed version
    """
    if "SWENo" in data:
        if data["SWENo"] is not None:
            data["SWENo"] = data["SWENo"].apply(
                lambda row: swe_hash(row) if row else row
            )
    return data


def swe_hash(swe_num):
    """
    Converts the **SWENo** field to a hash code represented in HEX
    :param swe_num: SWE number to be converted
    :return: Hash code version of SWE number
    """

    private_string = config("sec_str", default="")

    private_key = swe_num + private_string

    # Preparing plain text (SWENo) to hash it
    plaintext = private_key.encode()

    hash_algorithm = hashlib.sha3_256(plaintext)

    return hash_algorithm.hexdigest()


def add_fields(input_year, data, la_name):
    """
    Add YEAR, LA to exported dataframe

    :param input_year: A string of the year of return for the current file
    :param data: The dataframe to be cleaned
    :param la_name: LA name
    :return: Dataframe with year and LA added
    """
    data = convert_to_dataframe(data)
    data = add_year(data, input_year)
    data = add_la_name(data, la_name)
    return data


def degrade_data(data):
    """
    Degrade DoB to first of month and replace SWENo with hash code version

    :param data: The dataframe to be cleaned
    :return: Dataframe with degraded data
    """
    data = convert_to_datetime(data)
    data = degrade_dob(data)
    data = degrade_SWENo(data)
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
