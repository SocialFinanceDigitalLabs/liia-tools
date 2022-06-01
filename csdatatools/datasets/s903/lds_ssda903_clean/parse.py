import tablib
from datetime import datetime
import logging
import os

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"
lds_log_dir = r""
logging.basicConfig(filename=os.path.join(lds_log_dir, f"SSDA903_error_log_{start_time}.txt"),
                    format="\n%(name)s: %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def findfiles(input):
    """
    Locate the csv files within the given directory
    """
    try:
        for p in input:
            yield events.StartContainer(path=p)
            yield events.EndContainer(path=p)
    except Exception as ex:
        log.exception("Error trying to find files")


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event)
def add_filename(event):
    """
    Return the filename, so we can extract year information and use the filename when naming the output
    """
    try:
        yield event.from_event(event, filename=str(event.path.resolve().stem))
    except Exception as ex:
        log.exception("Error trying to add filename")


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event, error_function=pass_event)
def parse_csv(event):
    """
    Parse the csv and return the row number, column number, header name and cell value
    """
    try:
        with open(event.path, "rt") as f:
            yield events.StartContainer.from_event(event)
            data = tablib.import_set(f, format="csv")
            yield events.StartTable.from_event(event, headers=data.headers)
            for r_ix, row in enumerate(data):
                yield events.StartRow.from_event(event)
                for c_ix, cell in enumerate(row):
                    yield events.Cell.from_event(event, r_ix=r_ix, c_ix=c_ix, header=data.headers[c_ix], cell=cell)
                yield events.EndRow.from_event(event)
            yield events.EndTable.from_event(event)
    except Exception as ex:
        log.exception(f"Error trying to parse file: {event.filename}")
