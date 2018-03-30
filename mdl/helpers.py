# -*- coding: utf-8 -*-

import urllib


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
