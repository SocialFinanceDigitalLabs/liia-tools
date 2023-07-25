from liiatools.datasets.s251.lds_s251_clean import configuration

from sfdata_stream_parser import events


def test_add_matched_headers():
    config = configuration.Config(2022)
    event = events.StartTable(
        headers=[
            "Child ID",
            "Date of birth",
            "Gender",
            "Ethnicity",
            "Disability",
            "Category of need",
            "Date of last assessment",
            "Does the child have an EHCP",
            "Is the child UASC",
            "Number of missing episodes in current period of care",
            "Legal status",
            "Placement start date",
            "Placement end date",
            "Date of start of current care period",
            "Reason for placement change",
            "Number of placements in last 12 months",
            "Number of placements in current care period",
            "Placement type",
            "Provider type",
            "Procurement platform",
            "Procurement framework",
            "Ofsted URN",
            "Home postcode",
            "Placement postcode",
            "LA of placement",
            "Total committed cost accrued in FY to date",
            "Contribution from social care provider/s",
            "Contribution from health provider/s",
            "Contribution from education provider/s",
        ],
        filename="SSDA903_AD1.csv",
    )
    event_with_matched_headers = list(
        configuration.add_matched_headers(event, config=config)
    )[0]
    assert hasattr(event_with_matched_headers, "expected_columns")

    event = events.StartTable(
        headers=["incorrect", "header", "values"], filename="SSDA903_AD1.csv"
    )
    event_with_matched_headers = list(
        configuration.add_matched_headers(event, config=config)
    )
    assert not hasattr(event_with_matched_headers, "expected_columns")

    event = events.StartTable(headers=[""], filename="SSDA903_AD1.csv")
    event_with_matched_headers = list(
        configuration.add_matched_headers(event, config=config)
    )
    assert not hasattr(event_with_matched_headers, "expected_columns")

    event = events.StartTable(headers=[None], filename="SSDA903_AD1.csv")
    event_with_matched_headers = list(
        configuration.add_matched_headers(event, config=config)
    )
    assert not hasattr(event_with_matched_headers, "expected_columns")


def test_match_config_to_cell():
    event = events.Cell(header="Date of birth", filename="S251.csv")
    config_dict = {"Date of birth": {"date": "%d/%m/%Y", "canbeblank": False}}
    event_with_config = list(
        configuration.match_config_to_cell(event, config=config_dict)
    )[0]
    assert event_with_config.config_dict == config_dict["Date of birth"]

    event = events.Cell(header="Gender", filename="S251.csv")
    config_dict = {
        "Gender": {
            "category": [
                {"code": "1", "name": "Male"},
                {"code": "2", "name": "Female"},
            ],
            "canbeblank": False,
        }
    }
    event_with_config = list(
        configuration.match_config_to_cell(event, config=config_dict)
    )[0]
    assert event_with_config.config_dict == config_dict["Gender"]

    event = events.Cell(header="Gender", filename="S251.csv")
    config_dict = {}
    event_with_config = list(
        configuration.match_config_to_cell(event, config=config_dict)
    )[0]
    assert event_with_config == event

    event = events.Cell(header="Gender", filename="S251.csv")
    config_dict = None
    event_with_config = list(
        configuration.match_config_to_cell(event, config=config_dict)
    )[0]
    assert event_with_config == event

    event = events.Cell(header="Gender", filename="S251.csv")
    config_dict = 700
    event_with_config = list(
        configuration.match_config_to_cell(event, config=config_dict)
    )[0]
    assert event_with_config == event

    event = events.Cell(header="Gender", filename="S251.csv")
    config_dict = "random_string"
    event_with_config = list(
        configuration.match_config_to_cell(event, config=config_dict)
    )[0]
    assert event_with_config == event
