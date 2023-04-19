from enum import Enum


class ERROR_CAUSE(Enum):
    VALUE = 'Invalid value'
    TYPE = 'The type of data read does not match the expected data type'
    MISSING_TAG = 'Missing tag'
    TOO_MANY_TAGS = 'Found more tags than expected'
    GROUP_FORMAT = 'Not formatted as expected'
    MIN_RANGE = 'Below the value\'s expected minimum value'
    MAX_RANGE = 'Above the value\'s expected maximum value'
    NON_AGENCY_MANDATORY_MISSING = 'Mandatory tag for non-agency worker missing'
    LEAVER_MANDATORY_MISSING = 'Mandatory tag for leavers is missing'
    LEAVER_UNEXPECTED = 'Unexpected value present, should not be present for leavers'
    LEAVER_FTE = 'FTE must be 0 for leavers'


class ValidationError:

    def __init__(self, cause: ERROR_CAUSE, tag: str, sourceline: int = 0):
        self.cause = cause
        self.tag = tag
        self.sourceline = sourceline

    def __str__(self):
        return f'[Tag \'{self.tag}\' on line {self.sourceline}]: {self.cause.value}'
