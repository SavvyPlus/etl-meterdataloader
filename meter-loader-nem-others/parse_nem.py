import nemreader as nr
import config
import helpers



def generate_nemmdf_file_detail(row, source_file_id):
    """Extract and generate NEMMDF_FileDetails 100

    Args:
        row (string): string with comma sep

    Returns:
        type: Description of returned object.

    """
    values = helpers.parse_row(row)

    id = helpers.random_uuid()
    version_header = values[1]
    date_time = helpers.parse_datetime(values[-3])
    from_participant = values[-2]
    to_participant = values[-1]

    return [id, version_header, date_time, from_participant, to_participant, source_file_id]


def generate_nemmdf_stream_detail(row):
    """Extract and generate NEMMDF_StreamDetails 200

    Args:
        row (string): string with comma sep

    Returns:
        type: Description of returned object.

    """
    values = helpers.parse_row(row)

    id = helpers.random_uuid()
    nmi_configuration = values[2]
    register_id = values[3]
    mdm_data_stream_identifier = values[5]
    meter_serial_number = values[6]
    uom = values[7].upper()
    interval_length = values[8]
    next_scheduled_read_date = helpers.parse_datetime(values[-1])

    return [id, nmi_configuration, register_id, mdm_data_stream_identifier, meter_serial_number,
            uom, interval_length, next_scheduled_read_date]


def generate_nemmdf_stream_day(row, stream_id, source_file_id, stream_detail_id, interval_length):
    """Extract and generate NEMMDF_StreamDay 300

    Args:
        row (string): string with comma sep
        stream_id (string):
        source_file_id (stirng):
        stream_detail_id (string):
        interval_length (string):

    Returns:
        type: Description of returned object.

    """
    values = helpers.parse_row(row)

    interval_date = helpers.parse_datetime(values[1])
    update_date_time = helpers.parse_datetime(values[-2])
    msats_load_date_time = helpers.parse_datetime(values[-1])

    return [stream_id, interval_date, source_file_id, update_date_time, msats_load_date_time,
            stream_detail_id, interval_length]


def generate_nemmdf_quality_detail(row):
    """Extract and generate NEMMDF_QualityDetails inside 300

    Args:
        row (string): string with comma sep

    Returns:
        type: Description of returned object.

    """
    values = helpers.parse_row(row)

    id = helpers.random_uuid()
    quality_method = values[-5]
    reason_code = values[-4] if values[-4] else "-1"
    reason_description = values[-3]

    return [id, quality_method, reason_code, reason_description]


def generate_nemmdf_interval_datas(row, stream_id, interval_length, uom_id, quality_detail_id):
    """NEMMDF_IntervalData Inside 300

    Args:
        row (type): Description of parameter `row`.
        stream_id (type): Description of parameter `stream_id`.
        interval_length (type): Description of parameter `interval_length`.
        uom_id (type): Description of parameter `uom_id`.
        quality_detail_id (type): Description of parameter `quality_detail_id`.

    Returns:
        type: Description of returned object.

    """
    # TODO: uom_id ????
    records = []

    values = helpers.parse_row(row)
    interval_date = helpers.parse_datetime(values[1])
    interval_values = values[2:-5]

    for interval_number in range(len(interval_values)):
        record = [stream_id, interval_date, interval_number+1, interval_length,
                  interval_values[interval_number], uom_id, quality_detail_id]
        record = [helpers.normalize_csv_value(r) for r in record]
        records.append(record)

    return records


def generate_nemmdf_b2b_detail(row, stream_id, interval_date):
    # TODO: default transcode
    """NEMMDF_B2BDetailes 500 or default
    """
    id = helpers.random_uuid()
    if row:
        values = helpers.parse_row(row)

        trans_code = values[1]
        ret_service_order = values[2]
        read_date_time = helpers.parse_datetime(values[-2])
        index_read = values[-1]

        return [id, stream_id, interval_date, trans_code, ret_service_order, read_date_time, index_read]
    else:
        return [id, stream_id, interval_date, config.DEFAULT_TRANS_CODE, "", interval_date, ""]


def generate_nemmdf_stream(row):
    """NEMMDF_Stream 200
    """
    id = helpers.random_uuid()
    values = helpers.parse_row(row)
    nmi = values[1]
    nmi_suffix = values[4]
    meter_point_ref = nmi_suffix[0]
    stream_ref = nmi_suffix[1]

    return [id, nmi, nmi_suffix, meter_point_ref, stream_ref]





# def generate_nemmdf_quality_detail(row):
#     """Extract and generate NEMMDF_QualityDetails 400
#
#     Args:
#         row (string): string with comma sep
#
#     Returns:
#         type: Description of returned object.
#
#     """
#     values = row.split(",")
#
#     id = helpers.random_uuid()
#     quality_method = values[-3]
#     reason_code = values[-2] if values[-2] else -1
#     reason_description = values[-1]
#
#     return [id, quality_method, reason_code, reason_description]
