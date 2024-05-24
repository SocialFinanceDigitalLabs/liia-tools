import logging
import re
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic_yaml import parse_yaml_file_as

from liiatools.common.pipeline import Term
from liiatools.common.data import PipelineConfig
from liiatools.common.spec.__data_schema import DataSchema

__ALL__ = ["load_schema", "DataSchema", "Category", "Column"]

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent


@lru_cache
def load_pipeline_config():
    with open(SCHEMA_DIR / "pipeline.yml", "rt") as FILE:
        return parse_yaml_file_as(PipelineConfig, FILE)


@lru_cache
def load_schema(year: int, term: Term) -> DataSchema:
    pattern = re.compile(r"school_census_schema_(\d{4})(\.diff)?\.yml")

    # Build index of all schema files
    all_schema_files = list(SCHEMA_DIR.glob("school_census_schema_*.yml"))
    all_schema_files.sort()
    schema_lookup = []
    for fn in all_schema_files:
        match = pattern.match(fn.name)
        assert match, f"Unexpected schema name {fn}"
        schema_lookup.append((fn, int(match.group(1)), match.group(2) is not None))

    # Filter only those earlier than the year we're looking for
    schema_lookup = [x for x in schema_lookup if x[1] <= year]

    # If we have no schema files, raise an error
    if not schema_lookup:
        raise ValueError(f"No schema files found for year {year}")

    # Find the latest complete schema
    last_complete_schema = [x for x in schema_lookup if not x[2]][-1]

    # Now filter down to only include last complete and any diff files after that
    schema_lookup = [x for x in schema_lookup if x[1] >= last_complete_schema[1]]

    # We load the full schema
    logger.debug("Loading schema from %s", schema_lookup[0][0])
    full_schema = yaml.safe_load(schema_lookup[0][0].read_text())
    term_schema = full_schema[term]

    # Now loop over diff files and apply them
    for fn, _, _ in schema_lookup[1:]:
        logger.debug("Loading partial schema from %s", fn)
        try:
            diff = yaml.safe_load(fn.read_text())
            diff = diff[term]
        except KeyError:
            break
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing diff file {fn}") from e

        for key, diff_obj in diff.items():
            print(f"{year}, {term}, {diff_obj}")
            diff_type = diff_obj["type"]
            assert diff_type in (
                "add",
                "modify",
                "rename",
                "remove",
            ), f"Unknown diff type {diff_type}"
            path = key.split(".")
            parent = term_schema

            if diff_type in ["add", "modify"]:
                for item in path[:-1]:
                    parent = parent[item]
                parent[path[-1]] = diff_obj["value"]

            elif diff_type == "rename":
                dict = parent[path[0]][path[1]]
                dict[diff_obj["value"]] = dict.pop(path[-1])

            elif diff_type == "remove":
                if len(path) == 2:  # Remove columns
                    dict = parent[path[0]][path[1]]
                    [dict.pop(key) for key in diff_obj["value"]]
                elif len(path) == 1:  # Remove files
                    dict = parent[path[0]]
                    [dict.pop(key) for key in diff_obj["value"]]

    # Now we can parse the full schema into a DataSchema object from the dict
    return DataSchema(**term_schema)
