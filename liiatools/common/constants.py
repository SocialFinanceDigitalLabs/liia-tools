try:
    from enum import StrEnum
except ImportError:
    from backports.strenum import StrEnum


class ProcessNames(StrEnum):
    """Enum for process folders."""

    SESSIONS_FOLDER = "sessions"
    ARCHIVE_FOLDER = "archive"
    CURRENT_FOLDER = "current"
    EXPORT_FOLDER = "export"


class SessionNames(StrEnum):
    """Enum for session folders."""

    INCOMING_FOLDER = "incoming"
    CLEANED_FOLDER = "cleaned"
    ENRICHED_FOLDER = "enriched"
    DEGRADED_FOLDER = "degraded"
