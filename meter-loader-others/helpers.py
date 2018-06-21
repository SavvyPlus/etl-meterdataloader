import uuid
import datetime

from nemreader import nem_reader as nr


def random_uuid():
    """Return a random uuid
    Returns:
        type: string
    """
    return str(uuid.uuid4())[:13]


def parse_datetime(str, text=True, format="%Y-%m-%d %H:%M:%S"):
    """
    """
    dt = nr.parse_datetime(str)
    if text:
        return dt.strftime(format)
    else:
        return dt
