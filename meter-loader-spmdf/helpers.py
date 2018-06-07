# -*- coding: utf-8 -*-

import urllib
import datetime
from dateutil.parser import parse


def create_csv(vals, header=None, delimiter=",", file_path=None):
    """
    file_path + csv ex: abc/ef.csv

    if file_path is not none -> save to file
    else return bytes

    header is an array
    vals is an array of array
    *Note all header and vals are string type
    """
    vals_comma = [delimiter.join(val) for val in vals] # list of string val with comma
    full_content = "\n".join(vals_comma) # string full  vals with newline

    if header is not None:
        header = delimiter.join(header)
        full_content = header + "\n" + full_content

    byte_full_content = full_content.encode()

    if file_path:
        with open(file_path, 'wb') as w:
            w.write(byte_full_content)
            return file_path
    else:
        return byte_full_content


def unquote_url(url, encoding='utf-8', errors='replace'):
    """
    https://docs.python.org/3/library/urllib.parse.html
    using unquote_plus
    Example: unquote_plus('/El+Ni%C3%B1o/') yields '/El Niño/'.
    """
    return urllib.parse.unquote_plus(url, encoding=encoding, errors=errors)


def mins_bw_two_times(time1, time2):
    """
    time1 and time2 must be datetime.datetime type
    Ex: datetime.datetime(2016, 6, 7, 23, 45)
    """
    diff = time2 - time1
    return diff.seconds / 60


def get_time_now(text=True, format="%Y-%m-%d"):
    if text is True:
        return datetime.datetime.now().strftime(format)
    else:
        return datetime.datetime.now()


def check_like(value, pattern, like=True):
    """Check string match with a pattern
    Check pattern match at beginning or ending of string,
    or check pattern in value in any positions
    Args:
        value (string): string to check
        pattern (string): pattern to check match
        like (boolean): match LIKE or NOT LIKE, pattern in value or not
    Returns:
        type: boolean
    Examples:
        WHERE CustomerName LIKE 'a%'	Finds any values that starts with "a"
        WHERE CustomerName LIKE '%a'	Finds any values that ends with "a"
        WHERE CustomerName LIKE '%or%'	Finds any values that have "or" in any position
    """
    first = pattern[0]
    end = pattern[-1]
    # value = value.strip()
    if first == "%" and end == "%":
        return (pattern[1:-1] in value) == like
    elif first == "%":
        return value.endswith(pattern[1:]) == like
    elif end == "%":
        return value.startswith(pattern[:-1]) == like
    else:
        # check pattern in value
        return (pattern in value) == like


def check_spmdf_pattern(file_name, pattern):
    """
    rawdata*.csv
    DOE rawdata*.csv
    """
    patterns = pattern.split("*")
    return check_like(file_name, patterns[0]+"%") and check_like(file_name, "%"+patterns[-1])


def parse_date(s, text=True, format="%Y-%m-%d"):
    """Convert text to date, if not found date return "".
    Args:
        s (string): text to parse date
        text (boolean): return datetime type or datetime in format text
        format (string): format text to return for date
    Returns:
        type: string or datetime
    """
    try:
        date_parsed = parse(s)
        if text:
            return date_parsed.strftime(format)
        else:
            return date_parsed
    except Exception as e:
        return ""
