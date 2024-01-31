import pandas as pd
import numpy as np


def _time_between_date_series(
    later_date: pd.Series,
    earlier_date: pd.Series,
    years: bool = False,
    days: bool = False,
) -> pd.Series:
    """
    Returns the number of days between two date series.

    :param later_date: The later date.
    :param earlier_date: The earlier date.
    :param years: If True, returns the number of years between the two dates. The default is False.
    :param days: If True, returns the number of days between the two dates. The default is True.
    :returns: The number of days between the dates.
    """
    time = later_date - earlier_date
    time = time.dt.days

    if days:
        time = time.astype("Int64")
        return time

    elif years:
        time = (time / 365).apply(np.floor)
        time = time.astype("Int64")
        return time


def _filter_events(data: pd.DataFrame, day_column: str, max_days: int) -> pd.DataFrame:
    """
    Filters the data to only include events that occur within the specified maximum days.

    :param data: The data to filter.
    :param day_column: The column containing the date.
    :param max_days: The maximum number of days to include.
    :returns: The filtered data.
    """
    data = data[((data[day_column] <= max_days) & (data[day_column] >= 0))]
    return data
