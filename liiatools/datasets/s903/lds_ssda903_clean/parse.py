import tablib
import logging
from pathlib import Path
from datetime import datetime

from sfdata_stream_parser import events

log = logging.getLogger(__name__)


def parse_csv(input, la_log_dir):
    """
    Parse the csv and return the row number, column number, header name and cell value

    :param input: Location of file to be cleaned
    :param la_log_dir: Location to save the error log
    :return: List of event objects containing filename, header and cell information
    """
    start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
    extension = str(Path(input).suffix)
    filename = str(Path(input).resolve().stem)

    if extension in [".xml", ".xlsx", ".xlsm"]:
        assert extension == ".csv", "File not in the expected .csv format"

    elif extension == ".csv":
        with open(input, "rt") as f:
            yield events.StartContainer(filename=filename)
            data = tablib.import_set(f, format="csv")
            yield events.StartTable(filename=filename, headers=data.headers)
            for r_ix, row in enumerate(data):
                yield events.StartRow(filename=filename)
                for c_ix, cell in enumerate(row):
                    yield events.Cell(
                        filename=filename,
                        r_ix=r_ix,
                        c_ix=c_ix,
                        header=data.headers[c_ix],
                        cell=cell,
                    )
                yield events.EndRow(filename=filename)
            yield events.EndTable(filename=filename)
            yield events.EndContainer()

    else:
        with open(
            f"{Path(la_log_dir, filename)}_error_log_{start_time}.txt",
            "a",
        ) as f:
            f.write(
                f"File: '{filename}{extension}' not in any of the expected formats (csv, xml, xlsx, xlsm)"
            )
