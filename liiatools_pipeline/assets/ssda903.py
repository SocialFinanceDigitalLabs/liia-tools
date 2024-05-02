from dagster import asset
from decouple import config as env_config
from fs import open_fs
from liiatools.ssda903_pipeline.spec import load_pipeline_config


@asset
def pipeline_config():
    return load_pipeline_config()


@asset
def process_folder():
    output_location = env_config("OUTPUT_LOCATION", cast=str)
    return open_fs(output_location)


@asset
def incoming_folder():
    input_location = env_config("INPUT_LOCATION", cast=str)
    return open_fs(input_location)
