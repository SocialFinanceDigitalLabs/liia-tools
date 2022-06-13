import logging
import datetime
import os
from pathlib import Path
import yaml
from string import Template

from sfdata_stream_parser import events
from sfdata_stream_parser.filters.generic import streamfilter, pass_event
from sfdata_stream_parser.checks import type_check

from liiatools.datasets.s903.lds_ssda903_clean.columns import column_names
from liiatools.spec import s903 as s903_asset_dir
from liiatools.spec import common as common_asset_dir

log = logging.getLogger(__name__)

DEFAULT_CONFIG_DIR = Path(s903_asset_dir.__file__).parent
SHARED_CONFIG_DIR = Path(common_asset_dir.__file__).parent


@streamfilter(check=type_check(events.StartTable), fail_function=pass_event)
def add_table_name(event):
    """
    Match the loaded table name against one of the 10 903 file names
    """
    failed_matches = []
    for table_name, expected_columns in column_names.items():
        if set(event.headers) == set(expected_columns):
            return event.from_event(event, table_name=table_name)

        else:
            failed_matches.append("failed")

        if len(failed_matches) == len(column_names.items()):
            return event.from_event(
                event,
                match_error=f"Failed to find a set of matching columns headers for "
                f"file '{event.filename}' which contains column headers "
                f"{event.headers}",
            )


def inherit_table_name(stream):
    """
    Return the table name associated to a table to identify each row's table name
    """
    table_name = None
    for event in stream:
        try:
            if isinstance(event, events.StartTable):
                table_name = event.table_name
            elif isinstance(event, events.EndTable):
                event = event.from_event(event, table_name=table_name)
            elif table_name is not None:
                event = event.from_event(event, table_name=table_name)
            yield event
        except AttributeError:
            yield event


@streamfilter(check=type_check(events.Cell), fail_function=pass_event)
def match_config_to_cell(event, config):
    """
    Match the cell to the config file given the table name and cell header
    the config file should be a set of dictionaries for each table, headers within those tables
    and config rules for those headers
    """
    try:
        table_config = config[event.table_name]
        config_dict = table_config[event.header]
        return event.from_event(event, config_dict=config_dict)
    except (
        AttributeError,
        KeyError,
        TypeError,
    ):  # Raise in case there is no config item for the given table name and cell header
        return event


class Config(dict):
    def __init__(self, *config_files):
        super().__init__()

        if not config_files:
            config_files = ["DEFAULT_COLUMN_MAP", "DEFAULT_LA_MAP"]

        for file in config_files:
            if file == "DEFAULT_COLUMN_MAP":
                file = DEFAULT_CONFIG_DIR / "config.yml"
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
