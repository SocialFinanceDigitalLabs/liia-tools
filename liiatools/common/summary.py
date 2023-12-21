import logging
import pandas as pd
import re

from fs.info import Info
from fs.base import FS

from liiatools.common.data import (
    DataContainer,
    FileLocator,
)

logger = logging.getLogger()


# TODO: Make this work for .xlsx files


def _create_file_locator(source_fs: FS, file_path: str, file_info: Info) -> FileLocator:
    """
    Create FileLocator for a file in a given filesystem
    :param source_fs:  File system containing the input file
    :param file_path: Path to the input file
    :param file_info: File information
    :return: FileLocator with source, path and other information
    """
    file_locator = FileLocator(
        source_fs,
        file_path,
        metadata={
            "path": file_path,
            "name": file_info.name,
            "size": file_info.size,
            "modified": file_info.modified,
        },
    )

    return file_locator


def _create_locator_list(source_fs: FS) -> list[FileLocator]:
    """
    Create a FileLocator for each file in a given filesystem
    :param source_fs:  File system containing the input file
    :return: Stream of FileLocators with source, path and other information
    """
    source_file_list = source_fs.walk.info(namespaces=["details"])

    for file_path, file_info in source_file_list:
        if file_info.is_file:
            try:
                yield _create_file_locator(source_fs, file_path, file_info)
            except Exception as e:
                logger.error(f"Error creating FileLocator {file_path}")
                raise e


def _find_dataset_table_names(filename: str) -> tuple[str, str]:
    """
    Find the dataset and table names from a given filename e.g. /ssda903_Episodes.csv is the ssd903 dataset with
    table Episodes
    :param filename: Name of file
    :return: Tuple containing the dataset and table names
    """
    match = re.search(r"([a-zA-Z]*\d*)_([A-z]*\d*)", filename)
    dataset = match.group(1)
    table = match.group(2)
    return dataset, table


def _find_year_column(columns: list) -> str:
    """
    Find the year column in a list of columns e.g. YEAR, Year, year
    :param columns: A list of columns to search through
    :return: Name of the column that matches the year regex
    """
    for column in columns:
        year_column = re.search(r"year", column, re.I)
        if year_column:
            return year_column.group()


def _append_summary(summary_data: pd.DataFrame, summary_folder: FS) -> pd.DataFrame:
    """
    If a summary_processed_datasets.csv file already exists update and insert the newly processed data
    :param summary_data: Newly processed summary data
    :param summary_folder: File system containing the summary file
    :return: Updated dataframe containing new data
    """
    locator_list = _create_locator_list(summary_folder)
    for source in locator_list:
        with source.open("rb") as f:
            data = pd.read_csv(f)
            data = data.set_index(["Dataset", "Table", "LA"])
            data.columns = data.columns.astype(int)

            new_columns = [value for value in summary_data.columns if value not in data.columns]
            data = pd.concat([data, summary_data[new_columns]], axis=1).fillna(0)
            data.update(summary_data)

            data = data.reindex(sorted(data.columns), axis=1)
            return data


def process_summary(source_fs: FS, output_fs: FS):
    """
    Produce a summary report showing what data processed across years split by dataset, table and local authority
    e.g.

    Dataset | Table |   LA   | 2017 | 2018 | 2019
    ----------------------------------------------
    ssda903 |  OC2  | Barnet |  1   |  0   |   1

    :param source_fs: File system containing the input files
    :param output_fs: File system for the output files
    :return: None
    """
    locator_list = _create_locator_list(source_fs)
    summary_data = pd.DataFrame()
    for source in locator_list:
        with source.open("rb") as f:
            data = pd.read_csv(f)

            dataset, table = _find_dataset_table_names(source.name)
            if dataset in ["annex", "cin"]:
                data["Dataset"] = dataset + table
                data["Table"] = None
            else:
                data["Dataset"] = dataset
                data["Table"] = table

            data["Pivot"] = 1
            year_column = _find_year_column(data.columns)
            data = data.drop_duplicates(subset=["Dataset", "Table", "LA", year_column])

            data = pd.pivot_table(
                data,
                index=["Dataset", "Table", "LA"],
                values="Pivot",
                columns=year_column,
                aggfunc="count",
            )

            summary_data = pd.concat([summary_data, data]).fillna(0)

    summary_folder = output_fs.makedirs("SUMMARY", recreate=True)

    if len(list(summary_folder.walk.info(namespaces=["details"]))) == 1:
        summary_data = _append_summary(summary_data, summary_folder)

    summary_data = pd.DataFrame(summary_data.to_records())
    summary_data = DataContainer({"processed_datasets": summary_data})
    summary_data.export(summary_folder, "summary_", "csv")
