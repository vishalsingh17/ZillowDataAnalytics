import json
import boto3

s3_client = boto3.client('s3')
def lambda_handler(event, context):
    # TODO implement
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    print(source_bucket)
    print(object_key)
    target_bucket = "copy-of-json-data-2nd-bucket"

    copy_source = {'Bucket':source_bucket, 'Key': object_key}
    
    waiters = s3_client.get_waiter('object_exists')
    waiters.wait(Bucket=source_bucket, Key =object_key)
    
    s3_client.copy_object(Bucket=target_bucket, Key= object_key, CopySource= copy_source)
    return {
        'status_code' :200,
        'body': 'sucessfully_copied'
        
    }