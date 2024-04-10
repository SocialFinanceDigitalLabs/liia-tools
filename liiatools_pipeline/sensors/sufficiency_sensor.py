from dagster import RunRequest, run_status_sensor, sensor, DagsterRunStatus, RunConfig, DefaultSensorStatus

from liiatools_pipeline.jobs.ssda903 import ssda903_incoming
from liiatools_pipeline.jobs.sufficiency_903 import ssda903_sufficiency

'''
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    description="Adds in ONS and census data once 903 pipeline completes",
    request_job=ssda903_incoming,
)
@sensor(
    job=ssda903_sufficiency,
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
)
def sufficiency_sensor(context):
    yield RunRequest(
        run_key=None,
        run_config=RunConfig(),
    )'''