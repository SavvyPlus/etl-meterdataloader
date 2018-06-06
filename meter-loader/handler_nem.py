"""
Load meter data in NEM12 format.
Input is one file NEM12
-> save to file csv combines: IMD_15min and IMD_30min
"""
# TODO: check invalid after all process finish

import csv

import config
import format
import helpers
import s3_process
import nemreader as nr

# input_bucket = 'test-nem12.input'
# processing_bucket = 'test-nem12.processing'
# done_bucket = 'test-nem12.done'
# imd_15_bucket = 'test-nem12.imd-15.prod'
# imd_30_bucket = 'test-nem12.imd-30.prod'


processing_folder = "processing"
done_folder = "done"
athena_folder = "athena"
imd_15_folder = "imd_15min"
imd_30_folder = "imd_30min"


def process_file(csv_reader, file_name, local=True):
    """
    csv_reader = csv.reader(lines, delimiter=',')
    """
    all_readings = []
    all_readings_15min = []
    all_readings_30min = []
    nem = nr.parse_nem_rows(csv_reader, file_name)
    date = 'Y-m-d'
    for nmi in nem.readings:
        # nmi is MeterPointID
        for channel in nem.readings[nmi]:
            readings = nem.readings[nmi][channel]
            if not readings:
                continue
            # check IMD_15min or IMD_30min
            t_start = readings[0].t_start
            t_end = readings[0].t_end
            interval = int(helpers.mins_bw_two_times(t_start, t_end))
            if interval == 15:
                date, reading_15min  = format.imd_format(nmi, channel, readings, file_name, 15, config.PERIOD_15MIN)
                # date = result[0]
                # reading_15min = result[1]
                if reading_15min:
                    all_readings_15min = all_readings_15min + reading_15min
            elif interval == 30:
                # reading_30min = format.imd_30min(nmi, channel, readings, file_name)
                date, reading_30min = format.imd_format(nmi, channel, readings, file_name, 30, config.PERIOD_30MIN)
                # date = result[0]
                # reading_30min = result[1]
                if reading_30min:
                    all_readings_30min = all_readings_30min + reading_30min
            else:
                continue

    # TODO: return two files: 15 and 30
    # format.get_30_from_15(all_readings_15min)
    # all_readings = all_readings_15min + all_readings_30min
    # all_readings = format.merge_imd(all_readings_15min)

    all_readings_15min = format.merge_imd(all_readings_15min)
    all_readings_30min = format.merge_imd(all_readings_30min, 30, 48)


    if local:
        helpers.create_csv(all_readings_15min, config.IMD_HEADER, file_path="../output/IMD_15_mins.csv")
        readings_30_from_15 = format.get_30_from_15(all_readings_15min)
        if readings_30_from_15:
            all_readings_30min = readings_30_from_15 + all_readings_30min
        helpers.create_csv(all_readings_30min, config.IMD_HEADER, file_path="../output/IMD_30_mins.csv")
    else:
        # return date, helpers.create_csv(all_readings, config.IMD_HEADER)
        reading_15min_byte = helpers.create_csv(all_readings_15min, config.IMD_HEADER)
        readings_30_from_15 = format.get_30_from_15(all_readings_15min)
        if readings_30_from_15:
            all_readings_30min = readings_30_from_15 + all_readings_30min
        reading_30min_byte = helpers.create_csv(all_readings_30min, config.IMD_HEADER)
        return date, reading_15min_byte, reading_30min_byte


def handler(event, context):
    """
    upload file NEM12 to NEM12_INPUT_BUCKET
    -> process -> returm IMD csv: intervel 15min and 30min together
    """
    try:
        print ("Object added to: [%s]" % (event['Records'][0]['s3']['bucket']['name'],))
        file_name = event['Records'][0]['s3']['object']['key'].split('/')[-1]
        file_name = helpers.unquote_url(file_name)
        # move to PROCESSING_BUCKET
        s3_process.copy_file(input_bucket, file_name, processing_bucket, file_name)

        # get file from processing bucket
        obj = s3_process.get_object(processing_bucket, file_name)
        data = obj['Body'].read().decode('utf-8', 'ignore')
        lines = data.splitlines()
        csv_reader = csv.reader(lines, delimiter=',')
        # date, bytes_imd_csv = process_file(csv_reader, file_name, local=False)
        result = process_file(csv_reader, file_name, local=False)
        dst_key = s3_process.s3_key(result[0],"IMD_15_min_"+file_name)
        s3_process.put_file(imd_15_bucket, dst_key, result[1])

        dst_key = s3_process.s3_key(result[0],"IMD_30_min_"+file_name)
        s3_process.put_file(imd_30_bucket, dst_key, result[2])

        # move to NEM12_DONE_BUCKET
        s3_process.move_file(processing_bucket, file_name, done_bucket, file_name)
        print ("Finish process file: %s" % (file_name))

    except Exception as e:
        print ("Error: %s" % (str(e)))



def test_local():
    """
    create a CSV file
    """
    # file_name = "examples_input/NEM12#17092200001000000#GLOBALM#SAVBELGRAV.csv"
    file_name = "examples_input/NEM12#16042100594000000#GLOBALM#SPICK.csv"
    with open(file_name) as nmi_file:
        reader = csv.reader(nmi_file, delimiter=',')
        process_file(reader, file_name)
    # m = nr.read_nem_file(file_name)
    # print('Header:', m.header)
    # print('Transactions:', m.transactions)
    # for nmi in m.readings:
    #     for channel in m.readings[nmi]:
    #         print(nmi, 'Channel', channel)
    #         for reading in m.readings[nmi][channel][:]:
    #             print('', reading)

# test_local()
