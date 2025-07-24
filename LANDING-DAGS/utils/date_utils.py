from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Union

def date_add(
    date_input: Union[str, datetime], days: int = 0, format: str = "%Y-%m-%d"
) -> str:
    if isinstance(date_input, str):
        date_obj = datetime.strptime(date_input, format)
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise TypeError("date_input must be a string or datetime")

    new_date = date_obj + timedelta(days=days)
    return new_date.strftime(format)


def last_day(
    date_input: Union[str, datetime],
    months: int = 0,
    format: str = "%Y-%m-%d",
) -> str:
    if isinstance(date_input, str):
        date_obj = datetime.strptime(date_input, format)
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise TypeError("date_input must be a string or datetime")

    new_date = date_obj + relativedelta(months=months)
    last_day = new_date + relativedelta(months=1) - relativedelta(days=new_date.day)

    return last_day.strftime(format)


def last_year(
    date_input: Union[str, datetime],
    years: int = 0,
    format: str = "%Y-%m-%d",
) -> str:
    if isinstance(date_input, str):
        date_obj = datetime.strptime(date_input, format)
    elif isinstance(date_input, datetime):
        date_obj = date_input
    else:
        raise TypeError("date_input must be a string or datetime")

    # Adjust year by year_interval
    target_year = date_obj.year + years

    # Set to last day of the year (December 31st)
    last_year = datetime(target_year, 12, 31)

    return last_year.strftime(format)