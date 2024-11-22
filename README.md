# Zillow Data Analytics

## Objective
In this data engineering project, we will build and automate a python ETL process that would extract real estate properties data from Zillow Rapid API, loads it unto amazon s3 bucket which then triggers a series of lambda functions which then ultimately transforms the data, converts into a csv file format and load the data into another S3 bucket using Apache Airflow. Apache airflow will utilize an S3KeySensor operator to monitor if the transformed data has been uploaded into the aws S3 bucket before attempting to load the data into an amazon redshift. 
After the data is loaded into aws redshift, then we will connect amazon quicksight to the redshift cluster to then visualize the Zillow (rapid data) data.

## Tech Used
- Python
- RapidAPI
- AWS EC2
- S3
- Lambda
- Redshift
- Quicksight
- Airflow

## Architecture
<img width="706" alt="Architecture" src="https://github.com/user-attachments/assets/42728cfa-242c-4678-8912-1f15c865cd3b">


## How to run: 
install python on AWS ec2 instance
create virtual environment with python.
intall aws cli and configure it 
install apace-airflow
start apace-airflow (airflow standalone)

Step 2

add (dags_folder = /home/ubuntu/airflow/dags) config file in airflow.cfg 
disbale the load_examples = True to load_examples = False
now restart the airflow you will see the one dag

Step 3



