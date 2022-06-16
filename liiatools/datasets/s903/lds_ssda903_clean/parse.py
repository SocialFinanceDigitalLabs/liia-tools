import tablib
from datetime import datetime
import logging
import os
from pathlib import Path

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"


def log_config(lds_log_dir):
    return logging.basicConfig(
        filename=os.path.join(lds_log_dir, f"SSDA903_error_log_{start_time}.txt"),
        format="\n%(name)s: %(levelname)s: %(message)s",
    )


log = logging.getLogger(__name__)


def parse_csv(input):
    """
    Parse the csv and return the row number, column number, header name and cell value
    """
    filename = str(Path(input).resolve().stem)
    with open(input, "rt") as f:
        yield events.StartContainer(filename=filename)
        data = tablib.import_set(f, format="csv")
        yield events.StartTable(filename=filename, headers=data.headers)
        for r_ix, row in enumerate(data):
            yield events.StartRow(filename=filename)
            for c_ix, cell in enumerate(row):
                yield events.Cell(
                    filename=filename, r_ix=r_ix, c_ix=c_ix, header=data.headers[c_ix], cell=cell
                )
            yield events.EndRow(filename=filename)
        yield events.EndTable(filename=filename)
        yield events.EndContainer()
