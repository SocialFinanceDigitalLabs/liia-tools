"""This file contains constants and functions used in converting and merging XML data from CIN folder"""

from typing import Final, Dict, List
from decouple import config

import hashlib
import datetime as dt

# === CONSTANTS === #
LEA_DICT: Final[Dict[str, str]] = {
    '301': "Barking and Dagenham",
    '316': "Newham",
    '317': "Redbridge",
    '211': "Tower Hamlets",
    '311': "Havering",
    '320': "Waltham Forest"
}

ETHNICITY_GROUP_DICT: Final[Dict[str, str]] = {
    "WBRI": "White - British",
    "WIRI": "White - Irish",
    "WOTH": "Any Other White Background",
    "MWBC": "White and Black Caribbean",
    "MWBA": "White and Black African",
    "MWAS": "White and Asian",
    "MOTH": "Any Other Mixed background",
    "AIND": "Indian",
    "APKN": "Pakistani",
    "ABAN": "Bangladeshi",
    "AOTH": "Any Other Asian Background",
    "BCRB": "Black Caribbean",
    "BAFR": "Black African",
    "BOTH": "Any Other Black Background",
    "CHNE": "Chinese",
    "OOTH": "Any Other Ethnic Group",
    "REFU": "Declared not stated or Refused",
    "NOBT": "Information Not Yet Obtained"
}

ETHNICITY_DICT: Final[Dict[str, str]] = {
    "WBRI": "White - British",
    "WIRI": "White - Irish",
    "WOTH": "Any Other White Background",
    "MWBC": "White and Black Caribbean",
    "MWBA": "White and Black African",
    "MWAS": "White and Asian",
    "MOTH": "Any Other Mixed background",
    "AIND": "Indian",
    "APKN": "Pakistani",
    "ABAN": "Bangladeshi",
    "AOTH": "Any Other Asian Background",
    "BCRB": "Black Caribbean",
    "BAFR": "Black African",
    "BOTH": "Any Other Black Background",
    "CHNE": "Chinese",
    "OOTH": "Any Other Ethnic Group",
    "REFU": "Declared not stated or Refused",
    "NOBT": "Information Not Yet Obtained"
}

GENDER_DICT: Final[Dict[str, str]] = {
    '0': "Not known (gender has not been recorded)",
    '1': "male",
    '2': "female",
    '9': "not specified (indeterminate; unable to be classified as either male or female)"
}

ORG_ROLE_DICT: Final[Dict[str, str]] = {
    '1': "Senior Manager",
    '2': "Middle Manager",
    '3': "First Line Manager",
    '4': "Senior Practitioner",
    '5': "Case Holder",
    '6': "Qualified without cases"
}

QUAL_LEVEL_DICT: Final[Dict[str, str]] = {
    '1': "Undergraduate",
    '2': "Postgraduate",
    '3': "Other (for example any other qualification)"
}


# === FUNCTIONS === #
def swe_hash(worker: Dict[str, str]):
    """
    Converts the **SWENo** field to a hash code represented in HEX
    :param worker: A dictionary containing worker data
    :return: None
    """
    swe_num = worker['SWENo']

    private_string = config('sec_str', default='')

    private_key = swe_num + private_string

    # Preparing plain text (SWENo) to hash it
    plaintext = private_key.encode()

    hash_algorithm = hashlib.sha3_256(plaintext)

    worker['SWENo'] = hash_algorithm.hexdigest()


def convert_dates(worker: Dict[str, str]):
    """
    This function will:
    1) Add and compute the value for the following columns: **Age**, **RoleStartDate**, **RoleEndDate**, **DoB_year**
    (all of these values contain only their respective year, not the full date)
    2) Remove the **PersonBirthDate** field inside each worker dictionary
    :param worker: A dictionary containing worker data
    :return: None
    """
    date_format = '%Y-%m-%d'

    if 'PersonBirthDate' in worker:
        birth_date = worker['PersonBirthDate']
        worker['DoB_year'] = str(dt.datetime.strptime(birth_date, date_format).year)
        worker['Age'] = str(dt.datetime.today().year - dt.datetime.strptime(birth_date, date_format).year)

        del worker['PersonBirthDate']

    if 'RoleStartDate' in worker:
        worker['RoleStartDate'] = str(dt.datetime.strptime(worker['RoleStartDate'], date_format).year)

    if 'RoleEndDate' in worker:
        worker['RoleEndDate'] = str(dt.datetime.strptime(worker['RoleEndDate'], date_format).year)


def column_transfer(workers: List[Dict[str, str]], lea: str, year_census: str):
    """
    Adds new columns to the table.
    :param workers: A list of dictionaries containing worker data
    :param lea: Local Authority code
    :param year_census: Census year
    :return: None
    """
    for worker in workers:

        if lea in LEA_DICT:
            worker['LEAName'] = LEA_DICT[lea]
        else:
            worker['LEAName'] = ''

        # Adding ethnicity extra columns
        if 'Ethnicity' in worker:
            ethnicity = worker['Ethnicity']
            worker['Ethnicity_Group'] = ETHNICITY_GROUP_DICT[ethnicity]
            worker['Ethnicity_Compact'] = ETHNICITY_DICT[ethnicity]
        else:
            worker['Ethnicity_Group'] = ''
            worker['Ethnicity_Compact'] = ''

        # Adding gender extra column
        if 'GenderCurrent' in worker:
            worker['Gender'] = GENDER_DICT[worker['GenderCurrent']]
        else:
            worker['Gender'] = ''

        # Organisation role extra column
        if 'OrgRole' in worker:
            worker['OrgRoleName'] = ORG_ROLE_DICT[worker['OrgRole']]
        else:
            worker['OrgRoleName'] = ''

        # Qualification level
        if 'QualLevel' in worker:
            worker['QualLevelName'] = QUAL_LEVEL_DICT[worker['QualLevel']]
        else:
            worker['QualLevelNAme'] = ''

        worker['YearCensus'] = year_census
        worker['LEA'] = lea
