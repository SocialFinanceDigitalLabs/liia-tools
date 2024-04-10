from dagster import In, Nothing, Out, op
from fs.base import FS

from external_dataset.pipeline.pipeline import retrieve_dataset

from liiatools_pipeline.assets.external_dataset import external_data_folder


@op
def request_dataset(url):
    retrieve_dataset(url, external_data_folder())
