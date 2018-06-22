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
            record_100 = pn.generate_nemmdf_file_detail(row, source_file_id)
            record_100 = [helpers.normalize_csv_value(r) for r in record_100]
            NEMMDF_FileDetails.append(record_100)
        elif record_indicator == 200:

            # NEMMDF_StreamDetails
            record_200 = pn.generate_nemmdf_stream_detail(row)
            stream_detail_id = record_200[0]
            interval_length = record_200[-2]
            uom = record_200[-3]
            record_200 = [helpers.normalize_csv_value(r) for r in record_200]
            NEMMDF_StreamDetails.append(record_200)

            # NEMMDF_Stream
            record_200_stream = pn.generate_nemmdf_stream(row)

            if nmi and nmi != record_200_stream[1]:
                record_b2b = pn.generate_nemmdf_b2b_detail(None, stream_id, interval_date)
                record_b2b = [helpers.normalize_csv_value(r) for r in record_b2b]
                NEMMDF_B2BDetailes.append(record_b2b)

            nmi = record_200_stream[1]
            stream_id = record_200_stream[0]
            record_200_stream = [helpers.normalize_csv_value(r) for r in record_200_stream]
            NEMMDF_Stream.append(record_200_stream)


        elif record_indicator == 300:
            # NEMMDF_StreamDay
            record_300 = pn.generate_nemmdf_stream_day(row, stream_id, source_file_id, stream_detail_id, interval_length)
            interval_date = record_300[1]
            record_300 = [helpers.normalize_csv_value(r) for r in record_300]
            NEMMDF_StreamDay.append(record_300)

            # NEMMDF_QualityDetails
            record_300_quality = pn.generate_nemmdf_quality_detail(row)
            quality_detail_id = record_300_quality[0]
            record_300_quality = [helpers.normalize_csv_value(r) for r in record_300_quality]
            NEMMDF_QualityDetails.append(record_300_quality)

            # NEMMDF_IntervalData
            records_300_interval = pn.generate_nemmdf_interval_datas(row, stream_id, interval_length, uom, quality_detail_id)
            NEMMDF_IntervalData += records_300_interval
        elif record_indicator == 500:
            # NEMMDF_B2BDetailes
            record_500 = pn.generate_nemmdf_b2b_detail(row, stream_id, interval_date)
            record_500 = [helpers.normalize_csv_value(r) for r in record_500]
            NEMMDF_B2BDetailes.append(record_500)

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
