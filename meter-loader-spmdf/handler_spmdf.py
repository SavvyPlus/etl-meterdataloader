import sys
import urllib
import traceback

import format
import config
import helpers
import s3_process
import spmdfreader

from spmdf_config import SPMDF_CONFIG


processing_folder = "spmdf_processing"
done_folder = "spmdf_done"
error_folder = "spmdf_error"
athena_folder = "spmdf_athena"
imd_15_folder = "imd_15min"
imd_30_folder = "imd_30min"


def process_file(s, file_name, local=True, **kwargs):
    """
    """
    readings_15min, readings_30min = spmdfreader.process_spmdf(s, file_name, file_name, **kwargs)

    if not readings_15min and not readings_30min:
        return None, None, None

    file_path_15mins = "../output/%s_spmdf_15_mins.csv" % (file_name)
    file_path_30mins = "../output/%s_spmdf_30_mins.csv" % (file_name)

    if local is False:
        file_path_15mins = None
        file_path_30mins = None

    bytes_15mins = None
    bytes_30mins = None
    date = None

    if readings_15min:
        date = readings_15min[0][1]
        bytes_15mins = helpers.create_csv(readings_15min, config.IMD_HEADER, file_path=file_path_15mins)
        readings_30_from_15 = format.get_30_from_15(readings_15min)
        if readings_30_from_15:
            readings_30min = readings_30_from_15 + readings_30min

    if readings_30min:
        if date is None:
            date = readings_30min[0][1]
        bytes_30mins = helpers.create_csv(readings_30min, config.IMD_HEADER, file_path=file_path_30mins)

    return bytes_15mins, bytes_30mins, date


def handler(event, context):
    """
    """
    try:
        print ("Object added to: [%s]" % (event['Records'][0]['s3']['bucket']['name'],))
        meter_bucket = event['Records'][0]['s3']['bucket']['name']

        file_path = event['Records'][0]['s3']['object']['key']
        file_path = helpers.unquote_url(file_path)
        file_paths = file_path.split('/')
        source = file_paths[-2] # folder name
        file_name = file_paths[-1]

        key = file_path
        processing_key = "%s/%s/%s" % (processing_folder, source, file_name)
        done_key = "%s/%s/%s" % (done_folder, source, file_name)
        error_key = "%s/%s/%s" % (error_folder, source, file_name)

        # move to PROCESSING_BUCKET
        s3_process.move_file(meter_bucket, key, meter_bucket, processing_key)

        match_spmdf_configs = [k for k in SPMDF_CONFIG if SPMDF_CONFIG[k]["source"]
                               == source and helpers.check_spmdf_pattern(file_name, SPMDF_CONFIG[k]["pattern"])]
        if len(match_spmdf_configs) != 1:
            raise Exception("match more than one SPMDF_CONFIG or no match")

        params = SPMDF_CONFIG[match_spmdf_configs[0]]["params"]


        # get file from processing bucket
        obj = s3_process.get_object(meter_bucket, processing_key)
        data = obj['Body'].read().decode('utf-8', 'ignore')

        bytes_15mins, bytes_30mins, date = process_file(data, file_name, local=False, **params)

        if bytes_15mins:
            s3_process.put_file(meter_bucket, s3_process.partition_imd(file_name, date, athena_folder, "imd_15min"), bytes_15mins)
        if bytes_30mins:
            s3_process.put_file(meter_bucket, s3_process.partition_imd(file_name, date, athena_folder, "imd_30min"), bytes_30mins)

        # move to NEM12_DONE_BUCKET
        s3_process.move_file(meter_bucket, processing_key, meter_bucket, done_key)
        print ("Finish process file: %s" % (file_name))

    except Exception as e:
        s3_process.move_file(meter_bucket, processing_key, meter_bucket, error_key)
        print ("Error: %s" % (str(e)))
        traceback.print_exc(file=sys.stdout)
