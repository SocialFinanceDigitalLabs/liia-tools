from dagster import job

from liiatools_pipeline.ops import external_dataset as ed


@job
def external_incoming():
    ed.request_dataset()
