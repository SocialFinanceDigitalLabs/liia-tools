from dagster import job
from liiatools_pipeline.ops.sufficiency903 import dim_tables, ons_area, looked_after_child, ofsted_provider, postcode, episode, ofsted_inspection


@job
def ssda903_sufficiency():
    dim_tables()
    ons_area()
    looked_after_child()
    ofsted_provider()
    postcode()
    episode()
    ofsted_inspection()

