import logging
from datetime import datetime
import os

from sfdata_stream_parser import events, checks
from sfdata_stream_parser.parser import openpyxl
from sfdata_stream_parser.filters.generic import streamfilter, pass_event

start_time = f"{datetime.now():%d-%m-%Y %Hh-%Mm-%Ss}"

log = logging.getLogger(__name__)


def findfiles(input):
    """
    Locate the Excel files within the given directory
    """
    try:
        for p in input:
            yield events.StartContainer(path=p)
            yield events.EndContainer(path=p)
    except Exception as ex:
        log.exception("Error trying to find files")


@streamfilter(check=checks.type_check(events.StartContainer), fail_function=pass_event)
def add_filename_start(event):
    """
    Return the filename to save cleaned files using that name
    """
    try:
        yield event.from_event(event, filename=str(event.path.resolve().stem))
    except Exception as ex:
        log.exception("Error trying to add filename")


    except Exception as ex:
        log.exception(f"Error trying to parse file {event.filename}")
