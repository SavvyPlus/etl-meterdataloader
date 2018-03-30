"""
Load meter data in NEM12 format.
Input is one file NEM12
-> save to 2 csv files: IMD_15min.csv, IMD_30min.csv
"""


import csv

import config
import format
import helpers
# import s3_process
import nemreader as nr



def process_file(csv_reader, file_name, local=True):
    """
    csv_reader = csv.reader(lines, delimiter=',')
    """
    all_readings = []
    all_readings_15min = []
    all_readings_30min = []
    nem = nr.parse_nem_rows(csv_reader, file_name)
    for nmi in nem.readings:
        # nmi is MeterPointID
        for channel in nem.readings[nmi]:
            readings = nem.readings[nmi][channel]
            if not readings:
                continue
            # check IMD_15min or IMD_30min
            t_start = readings[0].t_start
            t_end = readings[0].t_end
            intervel = int(helpers.mins_bw_two_times(t_start, t_end))
            if intervel == 15:
                reading_15min = format.imd_15min(nmi, channel, readings, file_name)
                if reading_15min:
                    all_readings_15min = all_readings_15min + reading_15min
            elif intervel == 30:
                reading_30min = format.imd_30min(nmi, channel, readings,file_name)
                if reading_30min:
                    all_readings_30min = all_readings_30min + reading_30min
            else:
                continue
    format.get_30_from_15(all_readings_15min)
    # if local:
    #     helpers.create_csv(all_readings, config.IMD_HEADER, file_path="nem12151821.csv")
    # else:
    #     return helpers.create_csv(all_readings, config.IMD_HEADER)
    # TODO: return two files: 15 and 30



def test_local():
    """
    create a CSV file
    """
    file_name = "examples_input/NEM12#SA01YPLU160607VV#ENERGEXM#SAVVYPLU.csv"
    with open(file_name) as nmi_file:
        reader = csv.reader(nmi_file, delimiter=',')
        m = process_file(reader, file_name)
        # print('Header:', m.header)
        # print('Transactions:', m.transactions)
        # for nmi in m.readings:
        #     for channel in m.readings[nmi]:
        #         print(nmi, 'Channel', channel)
        #         for reading in m.readings[nmi][channel][-5:]:
        #             print('', reading)


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
        s3_process.copy_file(config.NEM12_INPUT_BUCKET, file_name, config.NEM12_PROCESSING_BUCKET, file_name)

        # get file from processing bucket
        obj = s3_process.get_object(config.NEM12_PROCESSING_BUCKET, file_name)
        data = obj['Body'].read().decode('utf-8', 'ignore')
        lines = data.splitlines()
        csv_reader = csv.reader(lines, delimiter=',')
        bytes_imd_csv = process_file(csv_reader, file_name, local=False)
        s3_process.put_file(config.NEM12_PROD_BUCKET, "IMD_15_30_min_"+file_name, bytes_imd_csv)

        # move to NEM12_DONE_BUCKET
        s3_process.move_file(config.NEM12_PROCESSING_BUCKET, file_name, config.NEM12_DONE_BUCKET, file_name)
        print ("Finish process file: %s" % (file_name))

    except Exception as e:
        print ("Error: %s" % (str(e)))



test_local()
