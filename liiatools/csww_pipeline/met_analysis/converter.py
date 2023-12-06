from typing import Final, Dict

import pandas as pd

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
    "NOBT": "Information Not Yet Obtained",
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
    "NOBT": "Information Not Yet Obtained",
}

GENDER_DICT: Final[Dict[str, str]] = {
    "0": "Not known (gender has not been recorded)",
    "1": "Male",
    "2": "Female",
    "9": "Not specified (indeterminate; unable to be classified as either male or female)",
}

ORG_ROLE_DICT: Final[Dict[str, str]] = {
    "1": "Senior Manager",
    "2": "Middle Manager",
    "3": "First Line Manager",
    "4": "Senior Practitioner",
    "5": "Case Holder",
    "6": "Qualified without cases",
}

SENIORITY_CODE_DICT: Final[Dict[str, str]] = {
    "1": "Newly qualified",
    "2": "Early career",
    "3": "Experienced",
    "4": "Senior",
    "5": "Agency",
}

QUAL_LEVEL_DICT: Final[Dict[str, str]] = {
    "1": "Undergraduate",
    "2": "Postgraduate",
    "3": "Other (for example any other qualification)",
}


def column_transfer(csww_df: pd.DataFrame):
    """
    Adds new columns to the table.
    :param csww_df: A dataframe containing worker data
    :return: Dataframe with new columns of mapped information
    """
    csww_df["Ethnicity_Group"] = csww_df["Ethnicity"].map(ETHNICITY_GROUP_DICT)
    csww_df["Ethnicity_Compact"] = csww_df["Ethnicity"].map(ETHNICITY_DICT)
    csww_df["Gender"] = csww_df["GenderCurrent"].map(GENDER_DICT)
    csww_df["OrgRoleName"] = csww_df["OrgRole"].map(SENIORITY_CODE_DICT)
    csww_df["QualLevelName"] = csww_df["QualLevel"].map(QUAL_LEVEL_DICT)

    return csww_df
