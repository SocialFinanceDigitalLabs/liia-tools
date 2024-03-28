"""
The Data module contains classes and functions for working with data in the pipeline and passing it between jobs.
"""
from .__config import ColumnConfig, PipelineConfig, TableConfig
from .__container import DataContainer, ErrorContainer, ProcessResult
from .__file import FileLocator, Metadata
