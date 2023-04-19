"""A script to write text to log files and terminal"""

import os
import platform
import sys
import datetime
from pathlib import Path
from typing import List, Final, Dict

import SWFtools.util.work_path as work_path

log_paths: Dict[str, str] = {}

TIME_FORMAT_FILE_NAME: Final[str] = "%d_%m_%Y_%H_%M_%S"
TIME_FORMAT_LOG_ENTRY: Final[str] = "%d/%m/%Y at %H:%M:%S"

OPERATING_SYSTEM: Final = platform.uname().system


# Creates a log file for runtime logs and writes a log header into the file
def initialise():
    time_start = datetime.datetime.now()

    # The log file will be stored in a dictionary for later use
    runtime_log_file_name = time_start.strftime(TIME_FORMAT_FILE_NAME) + ".txt"
    log_paths["runtime"] = os.path.join(work_path.runtime_log_files, runtime_log_file_name)

    try:
        with open(log_paths["runtime"], "w") as file:
            file.write("=" * 20 + '\n')
            file.write(f'Log file created on: {time_start.strftime(TIME_FORMAT_LOG_ENTRY)}\n')
            file.write(f'Operating System: {OPERATING_SYSTEM}\n')
            file.write("=" * 20 + '\n')
    except Exception as e:
        print("A critical error occurred when attempting to create and write into runtime log file.\nError message:\n")
        print(e)
        print('')
        sys.exit("The program closed after error occurred at runtime.")


def log_section_header(log_text: str, log_dir_name: str = "runtime", console_output: bool = False):
    thick_line = '=' * 5
    log_text = f'# {thick_line} {log_text} {thick_line} #'
    log(log_text, log_dir_name, console_output)


# Writes text into a log file that corresponds to the log id passed
def log(log_text: str | List[str], log_dir_name: str = "runtime", console_output: bool = False):
    """
    Writes text into the log file of an LA or to the application's runtime log file.
    :param log_text: A line or list of lines of text that will be written in the log file.
    :param log_dir_name: Directory name of the LA as it appears in the CIN folder. This will be used as a log id.
    If no value is passed it will write to the applications log file ('runtime')

    :param console_output: If it is set to 'True' then the passed text will also be printed to console. 'False' by default.
    :return: None
    """
    time_stamp = datetime.datetime.now()
    entry_time_stamp = time_stamp.strftime(TIME_FORMAT_LOG_ENTRY)

    try:
        # If a log file was created then it's id can be found inside log_files dictionary as a key
        if log_dir_name in log_paths:
            if console_output:
                __write_to_log_file_verbose(log_text, entry_time_stamp, 'a', log_paths[log_dir_name])
            else:
                __write_to_log_file(log_text, entry_time_stamp, 'a', log_paths[log_dir_name])

        # If log_id was not found in log_files dictionary, check if it matches to any LA directories and create
        # it in its respective directory
        elif work_path.is_la_directory(log_dir_name):
            file_path = os.path.join(work_path.flatfile_folder, log_dir_name, 'la_log',
                                     time_stamp.strftime(TIME_FORMAT_FILE_NAME) + ".txt")
            log_paths[log_dir_name] = file_path

            if console_output:
                __write_to_log_file_verbose(log_text, entry_time_stamp, 'w', log_paths[log_dir_name])
            else:
                __write_to_log_file(log_text, entry_time_stamp, 'w', log_paths[log_dir_name])

    except Exception as e:
        print("An error occurred when writing into the log file.\nError message:\n")
        print(e)


def __write_to_log_file(log_text: str | List[str], entry_time_stamp: str, mode: str, path: str):
    multiple_lines = type(log_text) is list

    with open(path, mode) as file:
        if multiple_lines:
            for line in log_text:
                file.write(f'[{entry_time_stamp}]: {line}\n')
        else:
            file.write(f'[{entry_time_stamp}]: {log_text}\n')


def __write_to_log_file_verbose(log_text: str | List[str], entry_time_stamp: str, mode: str, path: str):
    multiple_lines = type(log_text) is list

    with open(path, mode) as file:
        if multiple_lines:
            for line in log_text:
                file.write(f'[{entry_time_stamp}]: {line}\n')
                print(f'[{entry_time_stamp}]: {line}')
        else:
            file.write(f'[{entry_time_stamp}]: {log_text}\n')
            print(f'[{entry_time_stamp}]: {log_text}')


def log_footer(total_time: float):
    log_file_path = log_paths["runtime"]

    text = ["=" * 20, 'Process executed successfully.',
            f'Total time spent in seconds: {total_time}', f'Log file saved to: {log_file_path}',
            f'File name: {Path(log_file_path).stem}', "="*20]

    log(text)
