from airflow import DAG
from datetime import timedelta , datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

import json
import requests

with open('./airflow/config_api.json', 'r') as config_file:
    apihostkey = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%d/%m/%Y%H%M%S")
s3_bucket = "zillow-api-third-bucket"
def extract_zillow_data(**kwarg):
    url = kwarg.get('url')
    querystring = kwarg.get('querystring')
    headers = kwarg.get('headers')
    dt_string = "".join(kwarg.get("date_string").strip().split('/'))
    response = requests.get(url, headers= headers, params = querystring)
    response_data = response.json()
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f"response_data_{dt_string}.csv"
    with open(output_file_path, 'w') as output_file:
        json.dump(response_data, output_file, indent=4)
    output_list = [output_file_path, file_str]
    return output_list
    
default_args = {
    'owner': 'airflow',
    'depends_on_past' : False,
    'start_date': datetime(2024,8,2),
    'email' : ['vkkr800@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(seconds= 15),
}

with DAG('Zillow_Analytics_DAG',
        default_args= default_args,
        schedule_interval= '@daily',
        catchup= False,
        ) as dag:
    
        extract_zillow_data_var = PythonOperator(
            task_id = 'tsk_extract_zillow_data_var',
            python_callable= extract_zillow_data,
            op_kwargs= {'url':'https://zillow56.p.rapidapi.com/search', 'querystring' : {"location":"houston, tx","output":"json","status":"forSale","sortSelection":"priorityscore","listing_type":"by_agent","doz":"any"},'headers': apihostkey, 'date_string' : dt_now_string}
            )
        load_to_s3 = BashOperator(
            task_id = 'load_to_s3_bucket_zillowapibucket',
            bash_command = 'aws s3 mv {{ti.xcom_pull("tsk_extract_zillow_data_var")[0]}} s3://zillow-api-bucket-1st/', 
        )
        
        is_file_in_s3_available = S3KeySensor(
            task_id = "is_file_in_s3",
            bucket_key =  "{{ti.xcom_pull('tsk_extract_zillow_data_var')[1]}}",
            bucket_name=s3_bucket, 
            wildcard_match=False, 
            aws_conn_id='aws_s3_conn', 
            timeout = 60,
            poke_interval=5,
        )
        
        task_transfer_s3_to_redshift = S3ToRedshiftOperator(
            task_id="transfer_s3_to_redshift",
            aws_conn_id ="aws_s3_conn",
            redshift_conn_id = "conn_id_redshift",
            s3_bucket=s3_bucket,
            s3_key="{{ti.xcom_pull('tsk_extract_zillow_data_var')[1]}}",
            schema="PUBLIC",
            table="zillowdata",
            copy_options=["csv IGNOREHEADER 1"],
        )
        extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available >> task_transfer_s3_to_redshift