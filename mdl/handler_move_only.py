"""
File is moved from source to archive folder without further processing
"""


import config
import helpers
import s3_process


def move_only(filename):
    """
    do the action to move file from SOURCE_BUCKET -> ARCHIVE_BUCKET
    """
    try:
        print("Start moving file: ", filename)
        # move to ARCHIVE_BUCKET
        s3_process.move_file(config.MOVE_INPUT_BUCKET, filename, config.MOVE_ARCHIVE_BUCKET, filename)
    except Exception as e:
        print (e)
        # TODO handle error
        

def handler(event, context):
    """
    upload file to SOURCE_BUCKET -> file is moved to ARCHIVE_BUCKET
    """
    print ("Object added to: [%s]" % (event['Records'][0]['s3']['bucket']['name'],))
    filename = event['Records'][0]['s3']['object']['key'].split('/')[-1]
    filename = helpers.unquote_url(filename)
    move_only(filename)
