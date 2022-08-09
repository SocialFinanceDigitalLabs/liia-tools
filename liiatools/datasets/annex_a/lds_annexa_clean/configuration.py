import datetime
import logging
import os
from pathlib import Path
import yaml
from string import Template

from liiatools.datasets.annex_a.lds_annexa_clean.regex import parse_regex
from liiatools.datasets.shared_functions.common import inherit_property
from liiatools.spec import annex_a as annex_a_asset_dir
from liiatools.spec import common as common_asset_dir

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

log = logging.getLogger(__name__)

DEFAULT_CONFIG_DIR = Path(annex_a_asset_dir.__file__).parent
COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent


def _match_column_name(actual_value, expected_value, expected_expressions=None):
    """
    Matches an actual column name against an expected values. Can optionally take a list of expressions to test.
    Returns the expected value if a match is found, or None if no match is found.
    :param actual_value: Value that exists currently
    :param expected_value: Value that we expect
    :param expected_expressions: Optional list of (regex) expressions to test as well
    :return: The expected Value or None
    """
    assert actual_value is not None, "Must test a value"
    assert expected_value is not None, "Must test against a value"

    actual_value = actual_value.lower().strip()
    test_expected_value = expected_value.lower().strip()

    if actual_value == test_expected_value:
        return expected_value

    if expected_expressions:
        for ptn in expected_expressions:
            ptn = parse_regex(ptn)
            if ptn.match(actual_value):
                return expected_value

    return None


def configure_stream(stream, config):
    """
    Loading and matching the configuration with the loaded stream
    :param stream: Set of events to parse
    :param config: The loaded configuration
    :return: An updated set of events/stream with matched configuration
    """
    sheet_config = config["datasources"]
    cell_config = config["data_config"]
    stream = identify_blank_rows(stream)
    stream = add_sheet_name(stream, config=sheet_config)
    stream = inherit_property(stream, "sheet_name")
    stream = inherit_property(stream, "column_headers")
    stream = identify_cell_header(stream)
    stream = convert_column_header_to_match(stream, config=sheet_config)
    stream = match_property_config_to_cell(
        stream, config=cell_config, prop_name="category_config"
    )
    stream = match_property_config_to_cell(
        stream, config=sheet_config, prop_name="other_config"
    )
    return stream


def identify_blank_rows(stream):
    """
    Identify any blank rows in the dataset and assign a blank_row = "1" value to the corresponding
    StartRow, Cell and EndRow
    All Cell values between StartRow and EndRow are added to a list which is then checked to see if it contains
    only None values i.e. the row is blank. If row is blank return all events with a blank_row = "1" value,
    otherwise return all events without changes

    :param stream: A filtered list of event objects
    :return: An updated list of event objects with blank row information
    """
    row = None
    blank_row = []
    for event in stream:
        if isinstance(event, events.StartRow):
            row = [event]
        elif isinstance(event, events.EndRow):
            row.append(event)
            if blank_row == [None] * len(blank_row):
                yield from [event.from_event(event, blank_row="1") for event in row]
            else:
                yield from [event.from_event(event) for event in row]
            row = None
            blank_row = []
        elif isinstance(event, events.Cell):
            row.append(event)
            blank_row.append(event.value)
        elif row:
            row.append(event)
        else:
            yield event


@streamfilter(
    check=checks.type_check(events.StartTable),
    fail_function=pass_event,
    error_function=pass_event,
)
def add_sheet_name(event, config):
    """
    Match the loaded table against one of the Annex A sheet names using fuzzy matching with regex
    the column headers will be matched against the config, building a new list of matched headers
    which is then checked against the expected columns from the config
    If columns are matched but there are more columns than expected this will save those extra columns
    as event.extra_columns
    If no columns are matched for a table this will save the sheet name and column headers as event.match_error

    :param event: A filtered list of event objects of type StartTable
    :param config: A dictionary of keys containing sheet names and values containing headers
    :return: An updated list of event objects
    """
    for table_name, table_cfg in config.items():
        matched_names = []
        extra_columns = []

        # Creates a list of tuples holding (column_name, column_regex_list) for each configured column
        header_config = [
            (name, cfg.get("regex", [])) for name, cfg in table_cfg.items()
        ]

        for actual_column in event.column_headers:
            # Check the actual value against each of the configured columns and store values
            header_matches = [
                _match_column_name(actual_column, c[0], c[1]) for c in header_config
            ]

            # Filter checks to only those that matched
            matching_configs = [c for c in header_matches if c is not None]

            # Check if we have no, one or multiple configurations that match the actual value
            if len(matching_configs) == 0:
                extra_columns.append(actual_column)
            elif len(matching_configs) == 1:
                matched_names.append(matching_configs[0])
            else:
                raise ValueError(
                    "The actual column name matched multiple configured columns"
                )
        # If all of the expected columns are present, then we have a match
        if set(table_cfg.keys()) - set(matched_names) == set():
            return events.StartTable.from_event(
                event,
                sheet_name=table_name,
                extra_columns=extra_columns,
                matched_column_headers=matched_names,
            )
    return event


@streamfilter(check=checks.type_check(events.Cell), fail_function=pass_event)
def identify_cell_header(event):
    """
    Finds the correct column header for this Cell. The cell must have the property
    column_headers set otherwise this function returns the cell unmodified.

    Based on the cell's 'column_index' it looks up the corresponding header value in column_headers.

    Provides: column_header
    :param event: A filtered list of event objects of type Cell
    :return: An updated list of event objects
    """
    column_headers = event.get("column_headers")
    if column_headers:
        event = event.from_event(
            event, column_header=column_headers[event.column_index]
        )
    return event


@streamfilter(
    check=checks.type_check(events.Cell),
    fail_function=pass_event,
    error_function=pass_event,
)
def convert_column_header_to_match(event, config):
    """
    Converts the column header to the correct column header it was matched with e.g. Age -> Age of Child (Years)
    :param event: A filtered list of event objects of type Cell
    :param config: The loaded configuration to use
    :return: An updated list of event objects
    """
    column_config = config[event.sheet_name]
    for c in column_config:
        if c in str(event.column_header):
            return event.from_event(event, column_header=c)

        else:
            try:
                for r in column_config[c].get("regex", []):
                    p = parse_regex(r)
                    if p.match(str(event.column_header)) is not None:
                        return event.from_event(event, column_header=c)
            except AttributeError:  # Raised in case a config item empty which is acceptable
                pass
    return event.from_event(event, column_header="Unknown")


@streamfilter(
    check=checks.type_check(events.Cell),
    fail_function=pass_event,
    error_function=pass_event,
)
def match_property_config_to_cell(event, config, prop_name):
    """
    Match the cell to the config file given the sheet name and cell header
    the config file should be a set of dictionaries for each sheet, headers within those sheets
    and config rules for those headers

    :param event: A filtered list of event objects of type Cell
    :param config: The loaded configuration to use
    :param prop_name: The property name to match with the configuration
    :return: An updated list of event objects
    """
    try:
        sheet_config = config[event.sheet_name]
        config_dict = sheet_config[event.column_header]
        return event.from_event(event, **{prop_name: config_dict})
    except KeyError:  # Raised in case there is no property item for the given sheet name and cell header
        return event


class Config(dict):
    def __init__(self, *config_files):
        super().__init__()

        if not config_files:
            config_files = [
                "DEFAULT_DATA_SOURCES",
                "DEFAULT_DATA_MAP",
                "DEFAULT_DATA_CODES",
            ]

        for file in config_files:
            if file == "DEFAULT_DATA_SOURCES":
                file = DEFAULT_CONFIG_DIR / "annex-a-merge.yml"
            elif file == "DEFAULT_DATA_MAP":
                file = DEFAULT_CONFIG_DIR / "data-map.yml"
            elif file == "DEFAULT_DATA_CODES":
                file = COMMON_CONFIG_DIR / "LA-codes.yml"
            self.load_config(file, conditional=False)

        self["config_date"] = datetime.datetime.now().isoformat()
        try:
            self["username"] = os.getlogin()
        except OSError:
            # This happens when tests are not run under a login shell, e.g. CI pipeline
            pass

    def load_config(self, filename, conditional=False, warn=False):
        """
        Load configuration from yaml file. Any loaded configuration
        is only set if the values don't already exist in CONFIG.

        Files can contain ${} placeholders following the Python string.Template format.
        The context will include any keys already existing in the configuration, any keys
        from the current file - however, if these include placeholders, the placeholders
        will not be replaced. Finally, environment variables can be referenced with
        `os_environ_VARIABLE_NAME`.

        Keyword arguments:
        filename -- Filename to load from
        conditional -- If True, ignore file if it doesn't exist. If False, fail. (default False)
        """
        if conditional and not os.path.isfile(filename):
            if warn:
                log.warning("Missing optional file {}".format(filename))

            return

        with open(filename) as FILE:
            user_config = yaml.load(FILE, Loader=yaml.FullLoader)

        log.info(
            "Loading {} configuration values from '{}'.".format(
                len(user_config), filename
            )
        )

        environment_dict = {"os_environ_{}".format(k): v for k, v in os.environ.items()}

        variables = dict(self)
        variables.update(user_config)
        variables.update(environment_dict)

        with open(filename, "rt") as FILE:
            user_config_string = FILE.read()

        user_config_template = Template(user_config_string)
        user_config_string = user_config_template.substitute(variables)

        user_config = yaml.load(user_config_string, Loader=yaml.FullLoader)

        self.update(user_config)
