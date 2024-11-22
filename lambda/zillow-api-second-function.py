import json
import pandas as pd
import boto3

s3_client = boto3.client('s3')
def lambda_handler(event, context):
    # TODO implement
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    # print(source_bucket)
    # print(object_key)
    target_bucket = 'zillow-api-third-bucket'
    target_file_name = object_key[:-5]
    print('target_file_name',target_file_name)
    
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key=object_key)
    
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    # print('response', response)
    
    data = response['Body']
    # print("data", data)
    data = response['Body'].read().decode('utf-8')
    # print("decodred_data", data)
    f =[]
    data = json.loads(data)
    for i in data['results']:
        f.append(i)
    df = pd.DataFrame(f)
    selected_columns = manage
    df = df[selected_columns]
    
    csv_data = df.to_csv(index= False)
    bucket_name = target_bucket
    object_key = f'{target_file_name}.csv'
    s3_client.put_object(Bucket= bucket_name, Key= object_key, Body = csv_data)
    print('sucessfully uploaded')
    return {
        'statusCode': 200,
        'body': json.dumps('CSV conversion and s3 upload completed sucessfully')
    }