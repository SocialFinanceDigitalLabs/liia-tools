from sufficiency_data_transform.all_dim_and_fact import create_sufficiency_data
from dagster import job
from liiatools_pipeline.ops import ssda903


@job
def ssda903_sufficiency():
    print("Hi")
    create_sufficiency_data()