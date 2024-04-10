from dagster import job
from liiatools_pipeline.ops.sufficiency903 import ons_area


@job
def ssda903_sufficiency():
    ons_area()
