import numpy as np
import pandas as pd
import datetime
import pytest
from fs import open_fs

from liiatools.csww_pipeline.spec.samples import POPULATION
from liiatools.common.data import FileLocator
from liiatools.csww_pipeline.lds_csww_met_analysis import (
    converter,
    FTESum,
    growth_tables,
    pivotGen,
    seniority,
    validator,
)


@pytest.fixture
def create_test_worker():
    test_worker = pd.DataFrame(
        {
            "AgencyWorker": "0",
            "SWENo": "Oz2054309383",
            "FTE": 0.521371,
            "PersonBirthDate": datetime.date(1967, 1, 4),
            "GenderCurrent": "1",
            "Ethnicity": "REFU",
            "QualInst": "Institution Name",
            "QualLevel": "1",
            "StepUpGrad": "0",
            "OrgRole": "3",
            "RoleStartDate": datetime.date(1988, 4, 7),
            "StartOrigin": "9",
            "RoleEndDate": datetime.date(1990, 4, 7),
            "LeaverDestination": "1",
            "ReasonLeave": "2",
            "FTE30": 0.123456,
            "Cases30": 72,
            "WorkingDaysLost": 15.31,
            "ContractWeeks": 288.7,
            "FrontlineGrad": "1",
            "Absat30Sept": "0",
            "ReasonAbsence": "TRN",
            "CFKSSstatus": "1",
        },
        index=[0],
    )
    return test_worker


@pytest.fixture
def create_population_growth_data():
    test_population_growth_data = pd.DataFrame(
        {
            "LEAName": ["Barnet"],
            "2023": [1000],
            "2024": [2000],
            "2025": [3000],
        }
    )
    return test_population_growth_data


def test_column_transfer(create_test_worker):
    csww_df = converter.column_transfer(create_test_worker)
    assert csww_df["Ethnicity_Group"][0] == "Declared not stated or Refused"
    assert csww_df["Ethnicity_Compact"][0] == "Declared not stated or Refused"
    assert csww_df["Gender"][0] == "Male"
    assert csww_df["OrgRoleName"][0] == "Experienced"
    assert csww_df["QualLevelName"][0] == "Undergraduate"


def test_growth_tables():
    public_file = FileLocator(
        fs=open_fs(POPULATION.parent.as_posix()),
        file_location=r"/population_persons.csv",
    )
    growth_table = growth_tables.growth_tables(public_file)

    assert growth_table.columns.to_list() == [
        "LEAName",
        "2023",
        "2024",
        "2025",
        "2026",
        "2027",
        "2028",
        "2029",
        "2030",
        "2031",
        "2032",
        "2033",
    ]

    assert growth_table.values.tolist() == [
        [
            "Barnet",
            93650.231,
            93255.817,
            92768.5,
            92236.016,
            91505.535,
            90784.327,
            90202.917,
            89524.149,
            88943.837,
            88530.894,
            88236.426,
        ]
    ]


def test_get_year_columns():
    year_columns = growth_tables._get_year_columns(2023, 2)
    assert year_columns == [
        "2023",
        "2024",
        "2025",
    ]


def test_remove_invalid_worker(create_test_worker):
    # No PersonBirthDate if AgencyWorker == 0
    no_date = create_test_worker
    no_date["PersonBirthDate"] = np.nan
    no_date = validator.remove_invalid_worker_data(
        no_date, validator.NON_AGENCY_MANDATORY_TAG
    )
    assert no_date.empty

    # No StartOrigin if RoleStartDate not empty
    no_start_origin = create_test_worker
    no_start_origin["StartOrigin"] = np.nan
    no_start_origin = validator.remove_invalid_worker_data(
        no_start_origin, validator.NON_AGENCY_MANDATORY_TAG
    )
    assert no_start_origin.empty

    # No FTE if RoleEndDate not empty
    no_FTE = create_test_worker
    no_FTE["FTE"] = np.nan
    no_FTE = validator.remove_invalid_worker_data(
        no_FTE, validator.NON_AGENCY_MANDATORY_TAG
    )
    assert no_FTE.empty

    # No ReasonLeave if RoleEndDate not empty
    no_reason_leave = create_test_worker
    no_reason_leave["ReasonLeave"] = np.nan
    no_reason_leave = validator.remove_invalid_worker_data(
        no_reason_leave, validator.NON_AGENCY_MANDATORY_TAG
    )
    assert no_reason_leave.empty

    # No RoleEndDate if ReasonLeave not empty
    no_end_date = create_test_worker
    no_end_date["RoleEndDate"] = np.nan
    no_end_date = validator.remove_invalid_worker_data(
        no_end_date, validator.NON_AGENCY_MANDATORY_TAG
    )
    assert no_end_date.empty


def test_remove_invalid_xor():
    test_data = pd.DataFrame(
        {
            "column_1": ["string", np.nan, "string_3"],
            "column_2": ["string", "string_2", np.nan],
        }
    )

    test_data = validator._remove_invalid_xor(test_data, "column_1", "column_2")
    assert len(test_data) == 1


def test_add_new_or_not_column():
    test_data = pd.DataFrame(
        {
            "RoleStartDate": [datetime.date(2023, 4, 7), datetime.date(1988, 4, 7)],
            "Year": [2023, 2023],
        }
    )
    test_data = seniority.add_new_or_not_column(test_data)
    assert test_data["NewOrNot"].to_list() == ["New", "Not"]


def test_add_left_or_not_column():
    test_data = pd.DataFrame(
        {
            "RoleEndDate": [datetime.date(2023, 4, 7), datetime.date(1988, 4, 7)],
            "Year": [2023, 2023],
        }
    )
    test_data = seniority.add_left_or_not_column(test_data)
    assert test_data["LeftOrNot"].to_list() == ["Left", "Not"]


def test_add_seniority_column():
    test_data = pd.DataFrame(
        {
            "RoleStartDate": [
                datetime.date(2023, 4, 7),
                datetime.date(1988, 4, 7),
                datetime.date(2000, 4, 7),
                datetime.date(2001, 4, 7),
                datetime.date(1997, 4, 7),
            ],
            "AgencyWorker": ["1", "1", "0", "0", "0"],
            "OrgRole": ["3", "6", "5", "2", "1"],
            "Year": [2023, 2023, 2023, 2023, 2023],
        }
    )
    test_data = seniority.add_seniority_column(test_data)
    assert test_data["SeniorityCode"].to_list() == [1, 5, 2, 3, 4]


def test_create_demographic_table():
    test_data = pd.DataFrame(
        {
            "Year": [2023, 2023],
            "LA": ["Barnet", "Barnet"],
            "GenderCurrent": ["1", "1"],
            "Ethnicity": ["REFU", "REFU"],
            "SeniorityCode": [5, 5],
            "NewOrNot": ["Not", "Not"],
            "LeftOrNot": ["Not", "Not"],
            "FTE": [0.15, 0.25],
            "SWENo": ["Oz2054309383", "Of1829465987"],
        }
    )
    test_data = pivotGen.create_demographic_table(test_data)
    assert test_data["FTESum"][0] == 0.4
    assert test_data["SWENo_Count"][0] == 2


def test_convert_codes_to_names():
    test_data = pd.DataFrame(
        {
            "OrgRole": [1, 2, 3, 4, 5, 6],
            "SeniorityCode": [5, 4, 3, 2, 1, np.nan],
        }
    )
    test_data = seniority.convert_codes_to_names(test_data)
    assert test_data["SeniorityName"].to_list() == [
        "Agency",
        "Senior",
        "Experienced",
        "Early career",
        "Newly qualified",
        np.nan,
    ]

    assert test_data["OrgRoleName"].to_list() == [
        "Senior Manager",
        "Middle Manager",
        "First Line Manager",
        "Senior Practitioner",
        "Case Holder",
        "Qualified without cases",
    ]


def test_FTESum():
    test_data = pd.DataFrame(
        {
            "Year": [2023, 2023, 2023],
            "LA": ["Barnet", "Barnet", "Barnet"],
            "SeniorityCode": [5, 5, 4],
            "SeniorityName": ["Agency", "Agency", "Senior"],
            "NewOrNot": ["Not", "Not", "New"],
            "LeftOrNot": ["Not", "Not", "Not"],
            "FTE": [0.15, 0.25, 1],
        }
    )
    test_data = FTESum.FTESum(test_data)
    assert test_data["FTESum"][0] == 1.0
    assert test_data["FTESum"][1] == 0.4


def test_progressed():
    test_data = pd.DataFrame(
        {
            "Year": [2021, 2022, 2023],
            "SeniorityCode": [5, 5, 4],
            "SWENo": ["Oz2054309383", "Oz2054309383", "Oz2054309383"],
        }
    )
    test_data = seniority.progressed(test_data)
    assert test_data["Progress"].to_list() == [
        "Unknown",
        "No",
        "Progressed"
    ]


def test_seniority_forecast(create_population_growth_data):
    test_population_growth_data = create_population_growth_data

    test_fte_data = pd.DataFrame(
        {
            "LA": ["Barnet", "Barnet"],
            "Year": [2023, 2023],
            "SeniorityCode": [5, 4],
            "SeniorityName": ["Agency", "Senior"],
            "NewOrNot": ["Not", "New"],
            "LeftOrNot": ["Not", "Not"],
            "FTESum": [0.4, 1],
        }
    )

    test_data = seniority.seniority_forecast(test_fte_data, test_population_growth_data)
    assert test_data["2023"].to_list() == [0.4, 1.0]
    assert test_data["2024"].to_list() == [0.8, 2.0]
    assert test_data["2025"].to_list() == [1.2, 3.0]


def test_find_min_max_year_population_growth_table(create_population_growth_data):
    test_population_growth_data = create_population_growth_data

    min_year, max_year = seniority._find_min_max_year_population_growth_table(test_population_growth_data)
    assert min_year == 2023
    assert max_year == 2025
