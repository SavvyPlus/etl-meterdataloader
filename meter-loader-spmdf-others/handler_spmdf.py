import sys
import urllib
import traceback

import format
import config
import helpers
import s3_process
import spmdfreader

from spmdf_config import SPMDF_CONFIG


processing_folder = "spmdf_processing/other"
done_folder = "spmdf_done"
error_folder = "spmdf_error/other"
athena_folder = "spmdf_athena"
table_spmdf_details = "SPMDF_MeterDetails"


def process_file(s, file_name, local=True, **kwargs):
    """
    """
    df = spmdfreader.process_spmdf(s, file_name, file_name, **kwargs)
    df.columns = config.SPMDF_MeterDetails_Header

    df_grouped = df.groupby(config.SPMDF_MeterDetails_Header[:-1]).agg({'Max_Date':'max'})
    df_grouped = df_grouped.reset_index()

    bytes = df_grouped.to_csv(index=False)

    if local:
        file_path = "../output/%s_spmdf_details.csv" % (file_name)
        with open(file_path, 'w') as w:
            w.write(bytes)
            return file_path
    else:
        return bytes




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

        output = process_file(data, file_name, local=False, **params)
        output_bytes = output.encode()


        s3_process.put_file(meter_bucket, s3_process.partition_imd(file_name, "", athena_folder, table_spmdf_details), output_bytes)

        # move to NEM12_DONE_BUCKET
        s3_process.move_file(meter_bucket, processing_key, meter_bucket, done_key)
        print ("Finish process file: %s" % (file_name))

    except Exception as e:
        s3_process.move_file(meter_bucket, processing_key, meter_bucket, error_key)
        print ("Error: %s" % (str(e)))
        traceback.print_exc(file=sys.stdout)
