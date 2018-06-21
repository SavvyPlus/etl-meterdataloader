import helpers
import parse_nem as pn


def read_nem_detail(rows, source_file_id):
    """Short summary.

    Args:
        lines (list): list of rows in file nem
        source_file_id (string): name of the file nem

    Returns:
        type: Description of returned object.

    """
    # 100
    NEMMDF_FileDetails = []
    # 200
    NEMMDF_StreamDetails = []
    NEMMDF_Stream = []
    # 300
    NEMMDF_StreamDay = []
    NEMMDF_IntervalData = []
    NEMMDF_QualityDetails = []
    # 500 or default
    NEMMDF_B2BDetailes = []

    # validate file
    if rows[0][:3] != "100":
        print ("NEM Files must start with a 100 row: ", source_file_id)
        return {}

    # NEMMDF_StreamDetails
    stream_detail_id = ""
    interval_length = ""
    uom = ""

    # NEMMDF_StreamDay
    interval_date = ""

    # NEMMDF_Stream
    stream_id = ""

    # NEMMDF_QualityDetails
    quality_detail_id = ""

    nmi = ""


    for row in rows:
        record_indicator = int(row[:3])
        if record_indicator == 100:
            record = pn.generate_nemmdf_file_detail(row, source_file_id)
            record = [helpers.normalize_csv_value(r) for r in record]
            NEMMDF_FileDetails.append(record)
        elif record_indicator == 200:

            # NEMMDF_StreamDetails
            record = pn.generate_nemmdf_stream_detail(row)
            stream_detail_id = record[0]
            interval_length = record[-2]
            uom = record[-3]
            record = [helpers.normalize_csv_value(r) for r in record]
            NEMMDF_StreamDetails.append(record)

            # NEMMDF_Stream
            record = pn.generate_nemmdf_stream(row)

            if nmi and nmi != record[1]:
                record = pn.generate_nemmdf_b2b_detail(None, stream_id, interval_date)
                record = [helpers.normalize_csv_value(r) for r in record]
                NEMMDF_B2BDetailes.append(record)

            nmi = record[1]
            stream_id = record[0]
            record = [helpers.normalize_csv_value(r) for r in record]
            NEMMDF_Stream.append(record)


        elif record_indicator == 300:
            # NEMMDF_StreamDay
            record = pn.generate_nemmdf_stream_day(row, stream_id, source_file_id, stream_detail_id, interval_length)
            interval_date = record[1]
            record = [helpers.normalize_csv_value(r) for r in record]
            NEMMDF_StreamDay.append(record)

            # NEMMDF_QualityDetails
            record = pn.generate_nemmdf_quality_detail(row)
            quality_detail_id = record[0]
            record = [helpers.normalize_csv_value(r) for r in record]
            NEMMDF_QualityDetails.append(record)

            # NEMMDF_IntervalData
            records = pn.generate_nemmdf_interval_datas(row, stream_id, interval_length, uom, quality_detail_id)
            NEMMDF_IntervalData += records
        elif record_indicator == 500:
            # NEMMDF_B2BDetailes
            record = pn.generate_nemmdf_b2b_detail(row, stream_id, interval_date)
            record = [helpers.normalize_csv_value(r) for r in record]
            NEMMDF_B2BDetailes.append(record)

    results = {
        "NEMMDF_FileDetails": NEMMDF_FileDetails,
        "NEMMDF_StreamDetails": NEMMDF_StreamDetails,
        "NEMMDF_Stream": NEMMDF_Stream,
        "NEMMDF_StreamDay": NEMMDF_StreamDay,
        "NEMMDF_QualityDetails": NEMMDF_QualityDetails,
        "NEMMDF_IntervalData": NEMMDF_IntervalData,
        "NEMMDF_B2BDetailes": NEMMDF_B2BDetailes
    }

    return results
