from dagster import RunRequest, RunsFilter, DagsterRunStatus, sensor
from liiatools_pipeline.jobs.ssda903 import ssda903_incoming
from liiatools_pipeline.jobs.sufficiency_903 import ssda903_sufficiency


@sensor(job=ssda903_sufficiency)
def sufficiency_sensor(context):
    run_records = context.instance.get_run_records(
        filters=RunsFilter(
            job_name=ssda903_incoming.name,
            statuses=[DagsterRunStatus.SUCCESS],
        ),
        order_by="update_timestamp",
        ascending=False,
    )
    for run_record in run_records:
        yield RunRequest(run_key=run_record.dagster_run.run_id)
