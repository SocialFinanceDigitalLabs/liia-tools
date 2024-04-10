from dagster import job
from decouple import config as env_config
from liiatools_pipeline.ops import external_dataset as ed
from liiatools_pipeline.assets import external_dataset as ed_assets

@job
def external_incoming():
    ed.request_dataset(ed_assets.ons_data_location())
    ed.request_dataset(ed_assets.ofsted_data_location())
