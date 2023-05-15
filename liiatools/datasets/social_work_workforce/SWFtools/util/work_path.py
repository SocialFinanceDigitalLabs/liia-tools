import sys

import SWFtools.util.AppLogs as AppLogs
# Setting the paths to work and executing and using the scripts

import os

# Define filepaths
main_folder = r'/file_directories/'

# CIN Census files must be in one "LA" folder per LA, in the cin_folder
cin_folder = os.path.join(main_folder, 'cin')

# Flat files
flatfile_folder = os.path.join(main_folder, 'flatfiles')

# Outputs
output_folder = os.path.join(main_folder, 'outputs')

# Chris requests folder
request = os.path.join(main_folder, 'request')

# Workforce XML schema
XML_SCHEMA = os.path.join('spec', 'wf_xmlschema.xsd')

# Runtime log files
runtime_log_files = os.path.join(main_folder, 'log_files')

# Local Authority directories
la_directories = [folder for folder in os.scandir(cin_folder) if os.path.isdir(folder)]


def check_flatfiles_folder():
    """
    Checks that the 'flatfiles' folder contains all directories that are present in 'cin' folder and creates them if
    they are not present.
    :return: None
    """
    total_dirs = len(la_directories)
    checked_dirs = 0
    AppLogs.log(f'Checking {total_dirs} directories...', console_output=True)

    for directory in la_directories:
        la_flat_files_directory = os.path.join(flatfile_folder, directory.name)

        if os.path.exists(la_flat_files_directory):
            AppLogs.log(f'{directory.name} - Directory already exists', console_output=True)
        # If the folder does not exist, create it and add a log folder called 'la_log'
        else:
            try:
                os.mkdir(la_flat_files_directory)

                la_log_file_directory = os.path.join(la_flat_files_directory, "la_log")
                os.mkdir(la_log_file_directory)

                AppLogs.log(f'Created directory: {directory}', console_output=True)
            except PermissionError as pe:
                AppLogs.log(f'PERMISSION ERROR!\n{pe}', console_output=True)

                print('')
                sys.exit("The program closed after error occurred at runtime.")

        checked_dirs = checked_dirs + 1
        AppLogs.log(f'Checked {checked_dirs} of {total_dirs} directories.')


def is_la_directory(name: str) -> bool:
    for d in la_directories:
        if d.name == name:
            return True

    return False
