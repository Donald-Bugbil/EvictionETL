# Import necessary packages 

import pandas as pd
import pendulum
from airflow.decorators import task, dag
import logging
from database_config.database import database_initialize
from Clients.client import get_redshift_client, get_s3_client
from aws_config.aws import  BUCKET_NAME
import io


#task logger
task_logger=logging.getLogger('workflow.task')

@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2025,6,28,tz='UTC'),
    catchup=True,
    tags=['eviction_etl']
)

def workflow():
    """
    This workflow extract raw data from aws s3 bucket, clean, transform and load it back to redshift
    """

    @task()
    #establishing the database connection
    def database_initialization():
        try:
            database_initialize()
            task_logger.info(f'database is connected successfully:{True}')
            return True
        except Exception as e:
            task_logger.error(f"Trouble connecting to database:{e}")
            return None



    @task()
    #This task pulls the raw data, convetrs it into Bytes format and then into csv
    def extract():
        s3_client=get_s3_client()
        response=s3_client.get_object(Bucket=BUCKET_NAME, Key='Eviction_Notices_20250619.csv')['Body'].read()
        Bytes_format=io.BytesIO(response)
        data_frame=pd.read_csv(Bytes_format, low_memory=False)
        task_logger.info(data_frame)
        task_logger.info(f"DataFrame is generated successfully")
        return data_frame
    
    Initiaze_DB=database_initialization()
    extraction=extract()
workflow()
