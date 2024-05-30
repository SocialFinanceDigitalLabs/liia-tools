import logging
import pandas as pd
from pathlib import Path
# import cchardet as chardet
from datetime import datetime

import pandas.errors


# def file_encoding_audit(
#     data_folder: Path,
# ) -> pd.DataFrame:
#     """
#     Function takes in a folder path object, it then uses the cchardet library to fast detect the file encoding types
#
#     :param data_folder: Path object that is a folder containing files to be processed
#     :type data_folder: Path
#     :return: A Dataframe of low confidence encoded files
#     :rtype: pd.DataFrame
#     """
#
#     # TODO - Check csv encoding type of file, save to utf-8
#     # TODO - Check xml encoding type of file
#     # Save as an acceptable format
#     result_out = []
#
#     for cdf in data_folder.glob("**/*"):
#         if cdf.is_file() and "log" not in cdf.root:
#             with open(cdf, "rb") as f:
#                 msg = f.read()
#                 result = chardet.detect(msg)
#                 out = f"{cdf.parts[-3]}, {cdf.stem}, {result}"
#                 # this is messy.
#                 outt = (
#                     out.replace("}", "")
#                     .replace("{", "")
#                     .replace("confidence", "")
#                     .replace("encoding", "")
#                     .replace("'':", "")
#                 )
#                 result_out.append(outt)
#
#     # Save the outputs of the list generated by running cchardet on the file list,
#     # the result is then appended into a dataframe and filtered to return a list of files that
#     # have low confidence as to their encoding types.
#     encoding_series = pd.Series(result_out)
#
#     encoding_df = pd.DataFrame(encoding_series, columns=["file_name"])
#
#     # Split out dataframe
#     encoding_df[
#         ["local_authority", "file_name", "encoding", "confidence"]
#     ] = encoding_df.file_name.str.split(",", expand=True)
#
#     # Filter out log files and drop high confidence files types
#     encoded_df = encoding_df[
#         ~encoding_df["file_name"].str.contains("Logs")
#         & ~(encoding_df["confidence"].str.contains("1.0"))
#     ]
#
#     encoded_df.to_csv("encoding_audit.csv", encoding="utf-8")
#     return encoded_df


def delete_unrequired_files(input: str, drop_file_list: list, la_log_dir: str):
    """
    Deletes files if they match a substring name from list.

    :param input: should specify the input file location, including file name and suffix,
    and be usable by a Path function
    :type input: File
    :param drop_file_list: List of file names that will be removed
    :type drop_file_list: List
    :param la_log_dir: Path to the local authority's log folder
    :type la_log_dir: Path
    """
    input = Path(input)
    for dfl in drop_file_list:
        if dfl in input.stem:
            # logged in datapipe logs
            logging.info(f"{input.stem} removing file from processing not required.")
            save_unrequired_file_error(input, la_log_dir)
            input.unlink()
            raise Exception(
                f"{input.stem} has been deleted because it does not math the list of accepted files"
            )


def save_unrequired_file_error(input: Path, la_log_dir: str):
    """
    Saves unrequired file errors to a text file in the LA log directory

    :param input: The input file location, including file name and suffix, and be usable by a Path function
    :param la_log_dir: Path to the local authority's log folder
    :return: Text file containing the error information
    """
    filename = input.resolve().stem
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    with open(
        f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
        "a",
    ) as f:
        f.write(
            f"'{filename}' has been deleted because it does not match the list of accepted files"
        )
