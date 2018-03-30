import math
import pandas as pd

import config


def imd_15min(meter_point, channel, readings, file_name):
    """
    no_period = 96 or 48
    """
    final_readings = []

    first_read = readings[0]
    date = first_read.t_start.date().strftime("%Y-%m-%d")
    uom = first_read.uom
    quality_code = first_read.quality_method
    # check invalid data
    if quality_code not in ['A','E','F','S','X']:
        return False
    base_multiplier = config.UOM[uom.upper()]

    non_zero_readings = len(readings)
    zero_readings = config.PERIOD_15MIN  - non_zero_readings
    if zero_readings < 0:
        return False

    for i in range(zero_readings):
        record_zero = [meter_point, date, i+1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                       file_name, quality_code, uom, None, None, channel]
        record_zero = [str(i) for i in record_zero]
        final_readings.append(record_zero)

    next_period = zero_readings+1

    for reading in readings:
        Exp_KWH = 0
        Imp_KWH = 0
        Imp_KVARH = 0
        Exp_KVARH = 0

        read_value = reading.read_value * base_multiplier

        if channel[0] == "E":
            Exp_KWH = read_value
        elif channel[0] == "B":
            Imp_KWH = read_value
        elif channel[0] == "K":
            Imp_KVARH = read_value
        elif channel[0] == "Q":
            Exp_KVARH = read_value
        else:
            return False

        Net_KWH = Exp_KWH - Imp_KWH
        Net_KVARH = Exp_KVARH - Imp_KVARH
        KW = Net_KWH*(60/15)
        Net_KWH = KW/(60/15)
        KVA = math.sqrt(Net_KWH*Net_KWH + Net_KVARH*Net_KVARH)*(60/15)

        # check invalid data
        if Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH*Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH < 0:
            return False

        """
            MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
            Imp_KVARH	KW 	KVA	source_file_id	QualityCode
            UOM
            T_Start
            T_End
            Channel
            IntervalLength
        """

        record = [meter_point, date, next_period, Net_KWH, Net_KVARH, Exp_KWH, Imp_KWH, Exp_KVARH,
                  Imp_KVARH, KW, KVA, file_name, quality_code, uom, reading.t_start.strftime("%Y-%m-%d %H:%M:%S"),
                  reading.t_end.strftime("%Y-%m-%d %H:%M:%S"), channel]
        record = [str(i) for i in record]
        final_readings.append(record)
        next_period+=1
        # end for readings
    return final_readings


def imd_30min(meter_point, channel, readings, file_name):
    """
    """
    final_readings = []

    first_read = readings[0]
    date = first_read.t_start.date().strftime("%Y-%m-%d")
    uom = first_read.uom
    quality_code = first_read.quality_method
    base_multiplier = config.UOM[uom.upper()]

    non_zero_readings = len(readings)
    zero_readings = config.PERIOD_30MIN - non_zero_readings
    if zero_readings < 0:
        return False

    """
            MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
            Imp_KVARH	KW	KVA	KVA15	KW15	source_file_id	QualityCode
            UOM
            T_Start
            T_End
            Channel
    """
    for i in range(zero_readings):
        record_zero = [meter_point, date, i+1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                       file_name, quality_code, uom, None, None, channel]
        record_zero = [str(i) for i in record_zero]
        final_readings.append(record_zero)

    next_period = zero_readings+1
    for reading in readings:
        Exp_KWH = 0
        Imp_KWH = 0
        Imp_KVARH = 0
        Exp_KVARH = 0

        read_value = reading.read_value * base_multiplier

        if channel[0] == "E":
            Exp_KWH = read_value
        elif channel[0] == "B":
            Imp_KWH = read_value
        elif channel[0] == "K":
            Imp_KVARH = read_value
        elif channel[0] == "Q":
            Exp_KVARH = read_value
        else:
            return False

        Net_KWH = Exp_KWH - Imp_KWH
        Net_KVARH = Exp_KVARH - Imp_KVARH
        KW = Net_KWH*(60/30)
        Net_KWH = KW/(60/30)
        KVA = math.sqrt(Net_KWH*Net_KWH + Net_KVARH*Net_KVARH)*(60/30)

        # check invalid data
        if Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH*Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH < 0:
            return False
        """
                MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
                Imp_KVARH	KW	KVA	KVA15	KW15	source_file_id		QualityCode
                UOM
                T_Start
                T_End
                Channel
        """

        record = [meter_point, date, next_period, Net_KWH, Net_KVARH, Exp_KWH, Imp_KWH, Exp_KVARH,
                  Imp_KVARH, KW, KVA, 0, 0,file_name, quality_code, uom, reading.t_start.strftime("%Y-%m-%d %H:%M:%S"),
                  reading.t_end.strftime("%Y-%m-%d %H:%M:%S"), channel]
        record = [str(i) for i in record]
        final_readings.append(record)
        next_period+=1
        # end for readings
    return final_readings


def get_30_from_15(all_readings_15min):
    """
    """
    # df = pd.DataFrame.from_records(all_readings_15min, columns=config.IMD_15MIN_HEADER)
    # df = df.loc[(df['PeriodID']+1)/2 % 2 == 0]
    # print (df)

    for reading in all_readings_15min:
        reading[2] = (int(reading[2])+1)/2

    all_readings_15min = [reading for reading in all_readings_15min if (reading[2] / 0.5) % 2 == 0.0]

    for reading in all_readings_15min:
        reading[2] = int(reading[2])

    df = pd.DataFrame.from_records(all_readings_15min, columns=config.IMD_15MIN_HEADER)
    group_by = df.groupby(['MeterPointID', 'Date', 'PeriodID'])

    #
    # for reading in all_readings_15min:
    #     print (reading[2])
