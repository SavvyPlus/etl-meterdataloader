import boto3

s3 = boto3.client('s3')


def move_file(src_bucket, src_key, dst_bucket, dst_key):
    print("Moving [%s] to [%s] from [%s] to [%s]" % (src_key, dst_key, src_bucket, dst_bucket))
    s3.copy({'Bucket': src_bucket, 'Key': src_key}, dst_bucket, dst_key)
    s3.delete_object(Bucket=src_bucket, Key=src_key)
    print("Moved [%s] to [%s] from [%s] to [%s]" % (src_key, dst_key, src_bucket, dst_bucket))

def copy_file(src_bucket, src_key, dst_bucket, dst_key):
    print("Copying [%s] to [%s] from [%s] to [%s]" % (src_key, dst_key, src_bucket, dst_bucket))
    return s3.copy({'Bucket': src_bucket, 'Key': src_key}, dst_bucket, dst_key)
    print("Copied [%s] to [%s] from [%s] to [%s]" % (src_key, dst_key, src_bucket, dst_bucket))

def put_file(bucket, key, body):
    print("Put file [%s] to [%s]" % (key, bucket))
    return s3.put_object(Bucket=bucket, Key=key, Body=body)
    print("Finish put file [%s] to [%s]" % (key, bucket))

def get_object(bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)

def delete_object(bucket, key):
    print("Deleting file [%s] in [%s]" % (key, bucket))
    return s3.delete_object(Bucket=bucket, Key=key)
    print("Deleted file [%s] in [%s]" % (key, bucket))

# def create_bucket(bucket_name, region="ap-southeast-2"):
#     s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
#     return bucket_name
#
# def s3_key(ymd, filename):
#     parts = ymd.split("-")
#     key = "year=%s/month=%s/date=%s/%s" % (parts[0], parts[1], parts[2], filename)
#     return key
