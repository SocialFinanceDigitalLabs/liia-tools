from dagster import In, Nothing, Out, op
from fs.base import FS

from external_dataset.pipeline.pipeline import retrieve_dataset

from liiatools_pipeline.assets.external_dataset import external_data_folder


@op()
def request_dataset():
    retrieve_dataset("http://localhost:8080/sample.csv", external_data_folder)
