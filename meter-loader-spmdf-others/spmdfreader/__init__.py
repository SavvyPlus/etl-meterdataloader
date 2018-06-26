import math
import operator
from io import StringIO


import numpy as np
import pandas as pd


import format
import helpers



def process_spmdf(s, source_file_id, file_name, header_end_text=None, footer_start_text=None, fixed_column_vals={},
                  truncateNMI=False, map_col_names=None, flip_signs=[], **hp):
    """
    s is file string
    f = open(file_name,'rt')
    s = f.read()
    f.close()
    """
    # print (header_end_text)
    start_index = 0 if header_end_text is None else s.find(header_end_text)+len(header_end_text)

    if header_end_text is not None and start_index<len(header_end_text):    # note: find returns -1 if string not found
        error_text = "The text specified to indicate the end of the header is not found"
        print (error_text)
        return False, False
    else:
        end_index = len(s) if footer_start_text is None else start_index + s[start_index:].find(footer_start_text)
        if end_index<start_index:
            error_text = "The text specified to indicate the beginning of the footer is not found"
            print (error_text)
            return False, False

    csv_str = s[start_index:end_index]

    str_buf = StringIO(csv_str)

    # read as csv by pandas read_csv
    df = pd.read_csv(str_buf, **hp)

    for c in fixed_column_vals:
        df[c] = fixed_column_vals[c]

    if map_col_names is not None:
        df.rename(columns = map_col_names, inplace = True)

    for f in flip_signs:
        df[f][df[f] != 0] = df[f][df[f] != 0]*-1

    valid_fields = ['MeterRef','NMI','StreamRef','MeterSerialNumber',
                    'Date','Time','PeriodID','Timestamp','TimestampType','IntervalLength',
                    'Net_KWH','Net_KVARH','Exp_KWH','Imp_KWH','Exp_KVARH','Imp_KVARH','KW','KVA',
                    'MDPUpdateDateTime','QualityCode']

    # check for invalid fields
    fields = set(df.keys())
    if not fields.issubset(valid_fields):
        bad = fields - set(valid_fields)
        error_text = 'Invalid fields names encountered reading file (field names are case-sensitive): ' + ','.join(bad)
        print (error_text)
        return False, False

    if not 'IntervalLength' in fields:
        error_text = "The compulsory field IntervalLength is missing"
        print (error_text)
        return False, False

    if not (fields.issuperset(['Timestamp','TimestampType']) or fields.issuperset(['Date','PeriodID']) or fields.issuperset(['Date','Time','TimestampType'])):
        error_text = "Time Interval incorrectly specified. Needs either Timestamp+TimestampType, Date+PeriodID, or Date+Time"
        print (error_text)
        return False, False

    # TODO: check that both PeriodID and Time are not supplied
    if ('Timestamp' in fields and 'Date' in fields) or ('PeriodID' in fields and 'Time' in fields):
        error_text = "Duplicate time interval information specified. Needs either Timestamp+TimestampType, Date+PeriodID, or Date+Time"
        print (error_text)
        return False, False

    if not (fields.issuperset(['MeterRef']) or fields.issuperset(['NMI','StreamRef'])):
        error_text = "Meter information is incorrectly specified. Needs either MeterRef or NMI+StreamRef"
        print (error_text)
        return False, False

    if 'MeterRef' in fields and 'NMI' in fields:
        error_text = "Duplicate meter information specified. Needs either MeterRef only or NMI+StreamRef"
        print (error_text)
        return False, False

    # ensure that there is at least one value field
    if len(fields.intersection(['Net_KWH','Net_KVARH','Exp_KWH','Imp_KWH','Exp_KVARH','Imp_KVARH', 'KW', 'KVA']))==0:
        error_text = "There must be at least one value field included in the file"
        print (error_text)
        return False, False

    # validation checks
    if not set(df['IntervalLength']).issubset(set([15,30])):
        error_text = "IntervalLength must be integer with values 15 and 30"
        print (error_text)
        return False, False
    if 'QualityCode' in fields and not set(df['QualityCode']).issubset(set([None,'A','E','F','I','S','X'])):
        error_text = "QualityCode must be A,E,F,I,S or X. Found " + ','.join(set(df['QualityCode']) - set([None,'A','E','F','I','S','X']))
        print (error_text)
        return False, False

    # df['source_file_id'] = source_file_id

    # truncates NMI or MeterRef to first 10 characters
    # if 'NMI' in fields and truncateNMI:
    #     # df['NMI'] = map(lambda x: x[0:10], df['NMI'])
    #     df['NMI'] = df['NMI'].apply(lambda x: x[0:10])
    # if 'MeterRef' in fields and truncateNMI:
    #     # df['MeterRef'] = map(lambda x: x[0:10], df['MeterRef'])
    #     df['MeterRef'] = df['MeterRef'].apply(lambda x: x[0:10])
    # TODO: not truncate in SPMDF_MeterDetails

    # field-level validation
    if 'Imp_KWH' in fields and min(df['Imp_KWH']) < 0:
        error_text = "Negative Imp_KWH values detected. Consider flip_signs argument"
        print (error_text)
        return False, False

    if 'Exp_KWH' in fields and min(df['Exp_KWH']) < 0:
        error_text = "Negative Exp_KWH values detected. Consider flip_signs argument"
        print (error_text)
        return False, False

    if 'Imp_KVARH' in fields and min(df['Imp_KVARH']) < 0:
        error_text = "Negative Imp_KVARH values detected. Consider flip_signs argument"
        print (error_text)
        return False, False

    if 'Exp_KVARH' in fields and min(df['Exp_KVARH']) < 0:
        error_text = "Negative Exp_KVARH values detected. Consider flip_signs argument"
        print (error_text)
        return False, False

    if 'KVA' in fields and min(df['KVA']) < 0:
        error_text = "Negative KVA values detected"
        print (error_text)
        return False, False

    # ----
    no_rows = df["IntervalLength"].count()

    empty_series = pd.Series(np.array([""]*no_rows))

    if "MeterRef" in fields:
        MeterRef = df["MeterRef"]
        if "StreamRef" in fields:
            StreamRef = df["StreamRef"]
        else:
            StreamRef = df['MeterRef'].apply(lambda x: str(x)[11:])
        NMI = df['MeterRef'].apply(lambda x: str(x)[0:10])
    else:
        MeterRef = df['NMI']
        NMI = df['NMI'].apply(lambda x: str(x)[0:10])
        if "StreamRef" in fields:
            StreamRef = df["StreamRef"]
        else:
            StreamRef = df['NMI'].apply(lambda x: str(x)[11:])

    if "MeterSerialNumber" not in fields:
        MeterSerialNumber = empty_series
    else:
        MeterSerialNumber = df["MeterSerialNumber"]

    DLF_Code = empty_series
    TNI_Code = empty_series
    DLF_Value = empty_series
    TLF_Value = empty_series

    if "Date" in fields:
        Date = df["Date"]
    # elif "Time" in fields:
    #     Date = df["Time"]
    elif "Timestamp" in fields:
        Date = df["Timestamp"]
    else:
        Date = empty_series



    df = pd.concat([MeterRef, NMI, StreamRef, MeterSerialNumber, DLF_Code, TNI_Code, DLF_Value,
                    TLF_Value, Date], axis=1)

    return df



def read_spmdf(all_data):
    """
    """
    # readings_15min = sorted(readings_15min, key = operator.itemgetter(0, 1))
    all_data = sorted(all_data, key = operator.itemgetter(0,1))
    readings_15min = []
    readings_30min = []

    period = 1
    for row in all_data:
        # [MeterRef, NMI, StreamRef, Date, Timestamp, Exp_KVARH, Imp_KVARH, Exp_KWH, IntervalLength, QualityCode, file_id]

        Exp_KVARH = row[5]
        Imp_KVARH = row[6]
        Exp_KWH	= row[7]

        Imp_KWH = 0
        Net_KWH = 0
        Net_KVARH = 0
        KW = 0
        Net_KWH = 0
        KVA = 0
        KVA15 = 0
        KW15 = 0

        intervel_length = row[-3]
        file_name = row[-1]
        quality_method = row[-2]
        quality_number = None

        meter_point = row[0]

        # if row[0] == "" and row[1] != "" and row[2] != "":
        #     meter_point = str(row[1]) + "-" + str(row[2])

        if meter_point == "":
            meter_point = row[1]

        date = row[3]

        if row[3] == "":
            date = row[4]


        PeriodID = period

        record = [meter_point, date, PeriodID, Net_KWH, Net_KVARH, Exp_KWH, Imp_KWH, Exp_KVARH,
                  Imp_KVARH, KW, KVA, 0.0, 0.0, file_name, helpers.get_time_now(format="%Y-%m-%d %H:%M:%S"),
                  quality_method, quality_number]
        record = [str(i) for i in record]

        if intervel_length == 15:
            readings_15min.append(record)
            if period < 96:
                period+=1
            elif period == 96:
                period = 1

        if intervel_length == 30:
            record[-1] = "2"
            readings_30min.append(record)
            if period < 48:
                period+=1
            elif period == 48:
                period = 1

    return readings_15min, readings_30min



"""
if ('Timestamp' in fields and 'Date' in fields) or ('PeriodID' in fields and 'Time' in fields):
        error_text = "Duplicate time interval information specified. Needs either Timestamp+TimestampType, Date+PeriodID, or Date+Time"
        logger.error(error_text)
        DataDogAPI.Event.create(title="Duplicate time interval information:", text=error_text, alert_type="error",tags=tags)
        return (False,0)

-- Everything should have a MeterRef
	UPDATE SPMDF_Staging SET MeterRef = NMI+'-'+StreamRef
	WHERE ISNULL(MeterRef,'') = '' AND ISNULL(NMI,'') <> '' AND ISNULL(StreamRef,'') <> '' AND source_file_id = @sfid

	UPDATE SPMDF_Staging SET MeterRef = NMI
	WHERE ISNULL(MeterRef,'') = '' AND ISNULL(NMI,'') <> '' AND ISNULL(StreamRef,'') = '' AND source_file_id = @sfid

    	-- Get Date, PeriodID for each record
	UPDATE SPMDF_Staging SET TimestampType = 'PS', [Timestamp] = DATEADD(mi, -IntervalLength, [Timestamp])
	WHERE TimestampType = 'PE' AND [Timestamp] IS NOT NULL AND source_file_id = @sfid

	UPDATE SPMDF_Staging SET [Date] = CONVERT(date,[Timestamp]), PeriodID = 1 + DATEDIFF(mi, CONVERT(date,[Timestamp]), [Timestamp])/IntervalLength
	WHERE [Timestamp] IS NOT NULL AND [Date] IS NULL AND PeriodID IS NULL AND source_file_id = @sfid
"""
