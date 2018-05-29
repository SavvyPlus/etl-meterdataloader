# @Author: anh
# @Date:   2018-04-16T11:10:35+07:00
# @Email:  anh.phan@edge-works.net
# @Last modified by:   anh
# @Last modified time: 2018-04-16T17:14:21+07:00


import sys
import traceback
import math
# import pandas as pd

import config
import helpers


def imd_format(meter_point, channel, readings, file_name, intervel_length=15, period=96):
    """
    no_period = 96 or 48
    """
    quality_number = None
    if intervel_length == 30:
        quality_number = 2

    final_readings = []

    first_read = readings[0]
    date = first_read.t_start.date().strftime("%Y-%m-%d")
    uom = first_read.uom
    quality_code = first_read.quality_method
    # check invalid data
    if quality_code not in ['A','E','F','S','X']:
        return date, False
    base_multiplier = config.UOM[uom.upper()]

    non_zero_readings = len(readings)
    zero_readings = period  - non_zero_readings
    if zero_readings < 0:
        return date, False

    for i in range(zero_readings):
        # record_zero = [meter_point, date, i+1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        #                file_name, quality_code, uom, None, None, channel, intervel_length]
        record_zero = [meter_point, date, i+1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                       file_name, helpers.get_time_now(format="%Y-%m-%d %H:%M:%S"), quality_code, quality_number]
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
            return date, False

        # Net_KWH = Exp_KWH - Imp_KWH
        # Net_KVARH = Exp_KVARH - Imp_KVARH
        # KW = Net_KWH*(60/15)
        # Net_KWH = KW/(60/15)
        # KVA = math.sqrt(Net_KWH*Net_KWH + Net_KVARH*Net_KVARH)*(60/15)

        Net_KWH = 0
        Net_KVARH = 0
        KW = 0
        Net_KWH = 0
        KVA = 0

        # check invalid data
        # if Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH*Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH < 0:
        #     return date, False

        # record = [meter_point, date, next_period, Net_KWH, Net_KVARH, Exp_KWH, Imp_KWH, Exp_KVARH,
        #           Imp_KVARH, KW, KVA, 0.0, 0.0, file_name, quality_code, uom, reading.t_start.strftime("%Y-%m-%d %H:%M:%S"),
        #           reading.t_end.strftime("%Y-%m-%d %H:%M:%S"), channel, intervel_length]
        record = [meter_point, date, next_period, Net_KWH, Net_KVARH, Exp_KWH, Imp_KWH, Exp_KVARH,
                  Imp_KVARH, KW, KVA, 0.0, 0.0, file_name, helpers.get_time_now(format="%Y-%m-%d %H:%M:%S"),
                  reading.quality_method, quality_number]
        record = [str(i) for i in record]
        final_readings.append(record)
        next_period+=1
        # end for readings
    return date, final_readings


def merge_imd(all_readings, intervel_length=15, no_period=96):
    """
    keys are MeterPointID	Date
    """
    final_readings = []

    index = 0
    key1 = None
    key2 = None
    group_by_keys = []

    for row in all_readings:
        if index == 0:
            key1 = row[0]
            key2 = row[1]
            group_by_keys.append(row)
            index+=1
        else:
            if key1 == row[0] and key2 == row[1]:
                group_by_keys.append(row)
                index+=1
            else:
                # do merge here
                # reset after finish

                merges = merge_group_by_keys(group_by_keys, intervel_length, no_period)
                # if row[0] == "VCCCKC0021":
                #     print (merges)
                final_readings+= merges

                # reset
                index = 0
                key1 = row[0]
                key2 = row[1]
                group_by_keys.clear()
                group_by_keys.append(row)
                index+=1
    if group_by_keys:
        merges = merge_group_by_keys(group_by_keys, intervel_length, no_period)
        # if row[0] == "VCCCKC0021":
        #     print (merges)
        final_readings+= merges

    return final_readings


def merge_group_by_keys(group_by_keys, intervel_length, no_period):
    """
    """
    final_ones = []

    no_groups = len(group_by_keys)/no_period
    # TODO: check no_groups is correct or not

    start_indexes = [i*no_period for i in range(int(no_groups))]

    # if channel[0] == "E":
    #     Exp_KWH = read_value 5
    # elif channel[0] == "B":
    #     Imp_KWH = read_value 6
    # elif channel[0] == "K":
    #     Imp_KVARH = read_value 8
    # elif channel[0] == "Q":
    #     Exp_KVARH = read_value 7
    # else:
    #     return date, False


    for j in range(no_period):
        Exp_KWHs = [float(group_by_keys[si][5]) for si in start_indexes]
        Exp_KWH = max(Exp_KWHs)

        Imp_KWHs = [float(group_by_keys[si][6]) for si in start_indexes]
        Imp_KWH = max(Imp_KWHs)

        Imp_KVARHs = [float(group_by_keys[si][8]) for si in start_indexes]
        Imp_KVARH = max(Imp_KVARHs)

        Exp_KVARHs = [float(group_by_keys[si][7]) for si in start_indexes]
        Exp_KVARH = max(Exp_KVARHs)

        Net_KWH = Exp_KWH - Imp_KWH
        Net_KVARH = Exp_KVARH - Imp_KVARH
        KW = Net_KWH*(60/intervel_length)
        if Net_KWH == 0:
            Net_KWH = KW/(60/intervel_length)
        KVA = math.sqrt(Net_KWH*Net_KWH + Net_KVARH*Net_KVARH)*(60/intervel_length)
        # TODO:  check < 0 here

        group_by_keys[start_indexes[0]][5] = str(Exp_KWH)
        group_by_keys[start_indexes[0]][6] = str(Imp_KWH)
        group_by_keys[start_indexes[0]][8] = str(Imp_KVARH)
        group_by_keys[start_indexes[0]][7] = str(Exp_KVARH)

        group_by_keys[start_indexes[0]][3] = str(Net_KWH)
        group_by_keys[start_indexes[0]][4] = str(Net_KVARH)
        group_by_keys[start_indexes[0]][9] = str(KW)
        # group_by_keys[start_indexes[0]][3] = Net_KWH
        group_by_keys[start_indexes[0]][10] = str(KVA)

        final_ones.append(group_by_keys[start_indexes[0]])
        start_indexes = [i+1 for i in start_indexes]
    return final_ones



def get_30_from_15(all_readings_15min):
    """
    """
    len_readings_15min = len(all_readings_15min)
    final_ones = []

    for i in range(0, len_readings_15min+1, 2):
        try:
            first = all_readings_15min[i]
            second = all_readings_15min[i+1]
            # if (first[0] == second[0]) and (first[1] == second[1]) and (int(first[2])+1 == int(second[2])):
            if (first[0] == second[0]) and (int(first[2])+1 == int(second[2])):

                first[3] = float(first[3]) + float(second[3])
                first[4] = float(first[4]) + float(second[4])
                first[5] = float(first[5]) + float(second[5])
                first[6] = float(first[6]) + float(second[6])
                first[7] = float(first[7]) + float(second[7])
                first[8] = float(first[8]) + float(second[8])

                first[11] = max(float(first[10]), float(second[10]))
                first[12] = max(float(first[9]), float(second[9]))

                first[9] = (float(first[9]) + float(second[9])) / 2
                first[10] = (float(first[10]) + float(second[10])) / 2

                first[2] = int((int(first[2])+1)/2)
                first[-1] = 1 # TODO: check QualityNumber
                first = [str(fi) for fi in first]
                final_ones.append(first)
        except IndexError as e:
            pass
    return final_ones


def merge_imd_spmdf(all_readings, intervel_length=15, no_period=96):
    """
    keys are MeterPointID	Date
    """
    final_readings = []

    index = 0
    key1 = None
    key2 = None
    key3 = None
    group_by_keys = []

    for row in all_readings:
        if index == 0:
            key1 = row[0]
            key2 = row[1]
            key3 = row[2]
            group_by_keys.append(row)
            index+=1
        else:
            if key1 == row[0] and key2 == row[1] and key3 == row[2]:
                group_by_keys.append(row)
                index+=1
            else:
                # do merge here
                # reset after finish

                merges = merge_group_by_keys_spmdf(group_by_keys, intervel_length, no_period)
                # if row[0] == "VCCCKC0021":
                #     print (merges)
                final_readings+= merges

                # reset
                index = 0
                key1 = row[0]
                key2 = row[1]
                key3 = row[2]

                group_by_keys.clear()
                group_by_keys.append(row)
                index+=1
    if group_by_keys:
        merges = merge_group_by_keys_spmdf(group_by_keys, intervel_length, no_period)
        # if row[0] == "VCCCKC0021":
        #     print (merges)
        final_readings+= merges

    return final_readings


def merge_group_by_keys_spmdf(group_by_keys, intervel_length, no_period):
    """
    """
    final_ones = []

    no_groups = len(group_by_keys)/no_period
    # TODO: check no_groups is correct or not

    start_indexes = [i*no_period for i in range(int(no_groups))]

    if not start_indexes:
        start_indexes = [0]

    # if channel[0] == "E":
    #     Exp_KWH = read_value 5
    # elif channel[0] == "B":
    #     Imp_KWH = read_value 6
    # elif channel[0] == "K":
    #     Imp_KVARH = read_value 8
    # elif channel[0] == "Q":
    #     Exp_KVARH = read_value 7
    # else:
    #     return date, False


    for j in range(no_period):
        Exp_KWHs = [float(group_by_keys[si][5]) for si in start_indexes]
        Exp_KWH = sum(Exp_KWHs)

        Imp_KWHs = [float(group_by_keys[si][6]) for si in start_indexes]
        Imp_KWH = sum(Imp_KWHs)

        Imp_KVARHs = [float(group_by_keys[si][8]) for si in start_indexes]
        Imp_KVARH = sum(Imp_KVARHs)

        Exp_KVARHs = [float(group_by_keys[si][7]) for si in start_indexes]
        Exp_KVARH = sum(Exp_KVARHs)

        Net_KWH = Exp_KWH - Imp_KWH
        Net_KVARH = Exp_KVARH - Imp_KVARH
        KW = Net_KWH*(60/intervel_length)
        if Net_KWH == 0:
            Net_KWH = KW/(60/intervel_length)
        KVA = math.sqrt(Net_KWH*Net_KWH + Net_KVARH*Net_KVARH)*(60/intervel_length)
        # TODO:  check < 0 here

        group_by_keys[start_indexes[0]][5] = str(Exp_KWH)
        group_by_keys[start_indexes[0]][6] = str(Imp_KWH)
        group_by_keys[start_indexes[0]][8] = str(Imp_KVARH)
        group_by_keys[start_indexes[0]][7] = str(Exp_KVARH)

        group_by_keys[start_indexes[0]][3] = str(Net_KWH)
        group_by_keys[start_indexes[0]][4] = str(Net_KVARH)
        group_by_keys[start_indexes[0]][9] = str(KW)
        # group_by_keys[start_indexes[0]][3] = Net_KWH
        group_by_keys[start_indexes[0]][10] = str(KVA)

        final_ones.append(group_by_keys[start_indexes[0]])
        if len(start_indexes) == 1:
            break
        start_indexes = [i+1 for i in start_indexes]

    return final_ones





#
# def imd_30min(meter_point, channel, readings, file_name):
#     """
#     """
#     final_readings = []
#
#     first_read = readings[0]
#     date = first_read.t_start.date().strftime("%Y-%m-%d")
#     uom = first_read.uom
#     quality_code = first_read.quality_method
#     base_multiplier = config.UOM[uom.upper()]
#
#     non_zero_readings = len(readings)
#     zero_readings = config.PERIOD_30MIN - non_zero_readings
#     if zero_readings < 0:
#         return False
#
#     """
#             MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
#             Imp_KVARH	KW	KVA	KVA15	KW15	source_file_id	QualityCode
#             UOM
#             T_Start
#             T_End
#             Channel
#     """
#     for i in range(zero_readings):
#         record_zero = [meter_point, date, i+1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
#                        file_name, quality_code, uom, None, None, channel]
#         record_zero = [str(i) for i in record_zero]
#         final_readings.append(record_zero)
#
#     next_period = zero_readings+1
#     for reading in readings:
#         Exp_KWH = 0
#         Imp_KWH = 0
#         Imp_KVARH = 0
#         Exp_KVARH = 0
#
#         read_value = reading.read_value * base_multiplier
#
#         if channel[0] == "E":
#             Exp_KWH = read_value
#         elif channel[0] == "B":
#             Imp_KWH = read_value
#         elif channel[0] == "K":
#             Imp_KVARH = read_value
#         elif channel[0] == "Q":
#             Exp_KVARH = read_value
#         else:
#             return False
#
#         Net_KWH = Exp_KWH - Imp_KWH
#         Net_KVARH = Exp_KVARH - Imp_KVARH
#         KW = Net_KWH*(60/30)
#         Net_KWH = KW/(60/30)
#         KVA = math.sqrt(Net_KWH*Net_KWH + Net_KVARH*Net_KVARH)*(60/30)
#
#         # check invalid data
#         if Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH*Exp_KWH*Imp_KWH*Imp_KVARH*Exp_KVARH < 0:
#             return False
#         """
#                 MeterPointID	Date	PeriodID	Net_KWH	Net_KVARH	Exp_KWH	Imp_KWH	Exp_KVARH
#                 Imp_KVARH	KW	KVA	KVA15	KW15	source_file_id		QualityCode
#                 UOM
#                 T_Start
#                 T_End
#                 Channel
#         """
#
#         record = [meter_point, date, next_period, Net_KWH, Net_KVARH, Exp_KWH, Imp_KWH, Exp_KVARH,
#                   Imp_KVARH, KW, KVA, 0, 0,file_name, quality_code, uom, reading.t_start.strftime("%Y-%m-%d %H:%M:%S"),
#                   reading.t_end.strftime("%Y-%m-%d %H:%M:%S"), channel]
#         record = [str(i) for i in record]
#         final_readings.append(record)
#         next_period+=1
#         # end for readings
#     return final_readings

#
# def get_30_from_15(all_readings_15min):
#     """
#     """
#     # df = pd.DataFrame.from_records(all_readings_15min, columns=config.IMD_15MIN_HEADER)
#     # df = df.loc[(df['PeriodID']+1)/2 % 2 == 0]
#     # print (df)
#
#     for reading in all_readings_15min:
#         reading[2] = (int(reading[2])+1)/2
#
#     all_readings_15min = [reading for reading in all_readings_15min if (reading[2] / 0.5) % 2 == 0.0]
#
#     for reading in all_readings_15min:
#         reading[2] = int(reading[2])
#
#     df = pd.DataFrame.from_records(all_readings_15min, columns=config.IMD_15MIN_HEADER)
#     group_by = df.groupby(['MeterPointID', 'Date', 'PeriodID'])
#
#     #
#     # for reading in all_readings_15min:
#     #     print (reading[2])
