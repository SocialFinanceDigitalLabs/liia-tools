try:
    from enum import StrEnum
except ImportError:
    from backports.strenum import StrEnum


class ProcessNames(StrEnum):
    """Enum for process folders."""

    SESSIONS_FOLDER = "sessions"
    ARCHIVE_FOLDER = "archive"
    CURRENT_FOLDER = "cur"
    EXPORT_FOLDER = "exp"


class SessionNames(StrEnum):
    """Enum for session folders."""

    INCOMING_FOLDER = "incoming"
    CLEANED_FOLDER = "cleaned"
    DEGRADED_FOLDER = "degraded"
    ENRICHED_FOLDER = "enriched"
