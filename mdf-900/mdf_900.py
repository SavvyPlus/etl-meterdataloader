import boto3
import json

def handler(event, context):
    print("Object added to: [%s]" % (event['Records'][0]['s3']['bucket']['name'],))
