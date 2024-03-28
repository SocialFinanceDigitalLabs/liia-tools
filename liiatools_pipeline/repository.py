from dagster import repository
from liiatools.common._fs_serializer import register

from liiatools_pipeline.jobs.ssda903 import ssda903_incoming
from liiatools_pipeline.sensors.location_sensor import location_sensor

register()


@repository
def sync():
    """
    The repository definition for this etl Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [ssda903_incoming]
    schedules = []
    sensors = [location_sensor]

    return jobs + schedules + sensors
