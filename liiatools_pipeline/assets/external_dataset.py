from dagster import asset
from decouple import config as env_config
from fs import open_fs


@asset
def external_data_folder():
    external_data_location = env_config("EXTERNAL_DATA_LOCATION", cast=str)
    return open_fs(external_data_location)


@asset
def ons_data_location():
    return env_config("ONS_DATA_URL", cast=str)

@asset
def ofsted_data_location():
    return env_config("OFSTED_DATA_URL", cast=str)