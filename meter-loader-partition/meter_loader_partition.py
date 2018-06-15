# @Author: foamdino
# @Email: foamdino@gmail.com
# Modified by Dex
"""
This lambda handles the CREATE TABLE command, the initial MSCK command
and the ALTER TABLE ADD PARTITION command.
"""
import time
import boto3
import botocore
import os
from datetime import datetime, timedelta


s3 = boto3.client('s3')
athena = boto3.client('athena', region_name='ap-southeast-2') 


def check_msck_file(stack_name, from_bucket, file_key):
    """
    Check if the msck_file for thie source already exists
    returns:
      - ('ok', True) when file exists
      - ('ok', False) when file doesn't exist
      - ('fail', None) when there is an error
    """
    key_parts = file_key.split("/")
    provider = key_parts[1]
    prefix = key_parts[2]
    
    s3 = boto3.client('s3')
    bucket_name = os.environ['BucketName']
    key = f"msck-completed-files/{stack_name}-msck-completed-{provider}-{prefix}.txt" 
    try:
        print(f"Checking: {bucket_name}, {key}")
        s3.head_object(Bucket=bucket_name, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("msck file does not exist")
            return ('ok',False)
        else:
            print (e)
            return ('fail',None)
    else:
        print("msck file exists")
        return ('ok',True)


def create_table(,stack_name, database_name, from_bucket, file_key):
    """
    Create athena table

    Args:
      stack_name (string): name of the cloud formation stack to differentiate between test and production
      from_bucket (string): name of the bucket which the input file originated from
      database_name(string): name of the athena database
      file_key(string): the s3 key of the input file
    Returns:
      table name
    """

    print(f"Creating table in {database_name}")

    key_parts = file_key.split("/")
    provider = key_parts[1]
    prefix = key_parts[2]

    #get the input file
    output_file = s3.get_object(Bucket=from_bucket, Key=file_key)['Body'].read().decode("utf-8").splitlines(True)
    # read the readers, hardcode the data type of them to STRING
    schemas = output_file[0].replace("$", "DS").split(",")
    schemas_type = [schema+" STRING" for schema in schemas]
    schemas_sql = ", ".join(schemas_type)

    sql = f"CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{provider}_{prefix}_{stack_name.replace('-','_')} ({schemas_sql}) PARTITIONED BY (year int,month int,day int)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'WITH SERDEPROPERTIES ('serialization.format' = ',','field.delim' = ',' , 'quoteChar' = '\"')LOCATION 's3://{from_bucket}/athena/{provider}/{prefix}'TBLPROPERTIES ('has_encrypted_data'='false','skip.header.line.count'='1')"
    print(f"sql: {sql}")
    config = {
            'OutputLocation': 's3://' + from_bucket + '/' + database_name.replace('_','-'),
            'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    athena.start_query_execution(QueryString = sql,
                                     ResultConfiguration = config)
    print(f"Table {database_name}.{provider}_{prefix}_{stack_name} created") 
    return f"{provider}_{prefix}_{stack_name.replace('-','_')}"
    

def msck(stack_name, from_bucket,database_name, file_key):
    """
    Run MSCK command

    Args:
      stack_name (string): name of the cloud formation stack to differentiate between test and production
      from_bucket (string): name of the bucket which the input file originated from
      database_name(string): name of the athena database
      file_key(string): the s3 key of the input file
    Returns:
      nothing
    """
    
    table_name = create_table(database_name, from_bucket, file_key, stack_name)

    key_parts = file_key.split("/")
    provider = key_parts[1]
    prefix = key_parts[2]
    
    print ("MSCK starting...")
    print ("from_bucket: " + from_bucket)
    bucket_name = database_name.replace("_", "-") + '.log'
    print ("using: " + bucket_name)
    
    config = {
        'OutputLocation': 's3://' + bucket_name + '/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }

    # Query Execution Parameters
    sql = 'MSCK REPAIR TABLE ' + table_name
    context = {'Database': database_name}
    print(f"MSCK: {sql}")
    athena.start_query_execution(QueryString = sql, QueryExecutionContext = context, ResultConfiguration = config)
    
    # write msck file to main bucket
    try:
        s3.put_object(Bucket=os.environ['BucketName'], Key=f"msck-completed-files/{stack_name}-msck-completed-{provider}-{prefix}.txt" , Body="MSCK completed for %s" % (from_bucket,))
    except Exception as e:
        print (f"Error: {e} Unable to write msck file - msck command will run again")


def partition(stack_name, database_name, from_bucket, file_key):
    """
    Run ALTER TABLE command

    Args:
      stack_name (string): name of the cloud formation stack to differentiate between test and production
      database_name (string): name of the athena database
      from_bucket (string): name of the bucket which the input file originated from
      parsed_time (datetime): datetime created by parsing the input filename, used in the partition command

    Returns:
      nothing
    """
    print ("Adding partition...")
    key_parts = file_key.split("/")
    provider = key_parts[1]
    prefix = key_parts[2]
    day = key_parts[-2]
    month = key_parts[-3]
    year = key_parts[-4]
    
    name = database_name.replace("_", "-")
    bucket_name = name + '.log'
    print("using: " + bucket_name)

    athena = boto3.client('athena', region_name='ap-southeast-2')

    config = {
        'OutputLocation': 's3://' + bucket_name + '/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    # Query Execution Parameters
    sql = f"ALTER TABLE {database_name}.{provider}_{prefix}_{stack_name.replace('-','_')} ADD PARTITION ({year},{month},{day})" 
    print(f'Partition sql: {sql}')
    context = {'Database': database_name}
    athena.start_query_execution(QueryString = sql,
                                 QueryExecutionContext = context,
                                 ResultConfiguration = config)
    print("Partition added")


def handler(event, context):
    """
    Standard aws lambda handler function
    Handle input file. 
    
    Args:
      event (dict): the aws event that triggered the lambda
      context (dict): the aws context the lambda runs under
    """
    print(event['Records'][0]['s3']['object']['key'])

    stack_name = os.environ['StackName']
    database_name = os.environ['DatabaseName'].replace('-', '_')
    print ("stack_name: %s" % (stack_name),)
    from_bucket = event['Records'][0]['s3']['bucket']['name']
    print ("from_bucket: %s" % (from_bucket,))
    filekey = event['Records'][0]['s3']['object']['key'].replace("%3D","=")
    print("filekey: %s" %(filekey,))
    filename = filekey.split('/')[-1]
    print("filename: %s" %(filename,))

    msck_result = check_msck_file(stack_name, from_bucket, filekey)

    if msck_result[0] == 'ok' and not msck_result[1]:
        #msck file doesn't exist, which means this the first file comes in to a certain directory
        msck(stack_name, from_bucket, database_name, filekey)
        # leave some time to msck operation
        time.sleep(1)
    elif msck_result[0] == 'fail':
        # fail to check if msck file exists
        print ("unable to msck/partition due to error checking if msck file exists")

    # need to add partition no matter it's the first input file or not
    partition(stack_name, database_name, from_bucket, filekey)
