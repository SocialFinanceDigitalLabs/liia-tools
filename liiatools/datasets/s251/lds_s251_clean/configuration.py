import logging
import datetime
import os
from pathlib import Path
import yaml
from string import Template

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check

from liiatools.datasets.shared_functions.common import inherit_property
from liiatools.spec import s251 as s251_asset_dir
from liiatools.spec import common as common_asset_dir

log = logging.getLogger(__name__)

DEFAULT_CONFIG_DIR = Path(s251_asset_dir.__file__).parent
SHARED_CONFIG_DIR = Path(common_asset_dir.__file__).parent


@streamfilter(check=type_check(events.StartTable), fail_function=pass_event)
def add_matched_headers(event, config):
    """
    Match the loaded table headers against the S251 table headers

    :param event: A filtered list of event objects of type StartTable
    :param config: The loaded configuration to use
    :return: An updated list of event objects
    """
    expected_columns = list(config["placement_costs"].keys())
    if set(expected_columns).issubset(set(event.headers)):
        return event.from_event(event, expected_columns=expected_columns)
    return event


@streamfilter(check=type_check(events.Cell), fail_function=pass_event)
def match_config_to_cell(event, config):
    """
    Match the cell to the config file given the cell header
    the config file should be a set of dictionaries for with headers
    and config rules for those headers

    :param event: A filtered list of event objects of type Cell
    :param config: The loaded configuration to use
    :return: An updated list of event objects
    """
    try:
        config_dict = config[event.header]
        return event.from_event(event, config_dict=config_dict)
    except (
        AttributeError,
        KeyError,
        TypeError,
    ):  # Raise in case there is no config item for the given table name and cell header
        return event


def configure_stream(stream, config):
    """
    Loading and matching the configuration with the loaded stream

    :param stream: Set of events to parse
    :param config: The loaded configuration
    :return: An updated set of events/stream with matched configuration
    """
    stream = add_matched_headers(stream, config=config)
    stream = inherit_property(stream, "expected_columns")
    stream = match_config_to_cell(stream, config=config["placement_costs"])
    return stream


class Config(dict):
    def __init__(self, year=2023, *config_files):
        super().__init__()
        if not config_files:
            config_files = ["DEFAULT_COLUMN_MAP", "DEFAULT_LA_MAP"]

        for file in config_files:
            if file == "DEFAULT_COLUMN_MAP":
                file = DEFAULT_CONFIG_DIR / f"s251_schema_{year}.yml"
            elif file == "DEFAULT_LA_MAP":
                file = SHARED_CONFIG_DIR / "LA-codes.yml"
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
