import urllib
import uuid
import datetime

from nemreader import nem_reader as nr


def unquote_url(url, encoding='utf-8', errors='replace'):
    """
    https://docs.python.org/3/library/urllib.parse.html
    using unquote_plus
    Example: unquote_plus('/El+Ni%C3%B1o/') yields '/El Niño/'.
    """
    return urllib.parse.unquote_plus(url, encoding=encoding, errors=errors)


def random_uuid():
    """Return a random uuid
    Returns:
        type: string
    """
    return str(uuid.uuid4())[:13]


def parse_datetime(str, text=True, format="%Y-%m-%d %H:%M:%S"):
    """
    """
    str = str.strip()
    if not str:
        return ""
    dt = nr.parse_datetime(str)
    if text:
        return dt.strftime(format)
    else:
        return dt


def parse_row(row):
    values = row.split(",")
    return [v.strip() for v in values]


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


def normalize_csv_value(value, delimiter=","):
    """pre-process text before saved to CSV file
    """
    value_str = str(value)
    if delimiter in value_str:
        value_str = value_str.replace(",", "\\,")
    return value_str
