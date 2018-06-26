import config
import helpers
import s3_process
import read_nem as rn


processing_folder = "processing/others"
done_folder = "done"
# error_folder = "error/others"
athena_folder = "athena"



def handler(event, context):
    try:
        print ("Object added to: [%s]" % (event['Records'][0]['s3']['bucket']['name'],))
        meter_bucket = event['Records'][0]['s3']['bucket']['name']
        key_name = event['Records'][0]['s3']['object']['key']
        key_name = helpers.unquote_url(key_name)
        file_name = key_name.split('/')[-1]

        processing_key = "%s/%s" % (processing_folder, file_name)
        done_key = "%s/%s" % (done_folder, file_name)
        # error_key = "%s/%s" % (error_folder, file_name)
        # move to PROCESSING_BUCKET
        s3_process.move_file(meter_bucket, key_name, meter_bucket, processing_key)
        # get file from processing bucket
        obj = s3_process.get_object(meter_bucket, processing_key)
        data = obj['Body'].read().decode('utf-8', 'ignore')
        rows = data.splitlines()

        results, interval_date = rn.read_nem_detail(rows, file_name)
        interval_date = interval_date[:10]

        for r in results:
            try:
                csv_bytes = helpers.create_csv(results[r], header=config.HEADERS[r])
                athena_key = s3_process.s3_key(interval_date, file_name, athena_folder, r)
                s3_process.put_file(meter_bucket, athena_key, csv_bytes)
            except:
                print ("erorr %s in %s" % (r, file_name))

        # move to NEM12_DONE_BUCKET
        s3_process.move_file(meter_bucket, processing_key, meter_bucket, done_key)
        print ("Finish process file: %s" % (file_name))

    except Exception as e:
        print ("Error: %s" % (str(e)))
