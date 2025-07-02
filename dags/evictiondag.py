# Import necessary packages 

import pandas as pd
import pendulum
from airflow.decorators import task, dag
import logging
from database_config.database import database_initialize
from Clients.client import get_redshift_client, get_s3_client
from aws_config.aws import  BUCKET_NAME
from utilities.funtions import boolens, clean_zip, state, clean_city, extract_lat_lon, extract_lat_lon_from_shape
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
    

    @task()
    def transform(data_frame):
        new_data_frame=data_frame.copy()
        
        #Drop the columns that has no influence to the data
        
        new_data_frame.drop(['Supervisor District', 'Neighborhoods - Analysis Boundaries', 'SF Find Neighborhoods','Current Police Districts', 
                            'Current Supervisor Districts','Analysis Neighborhoods','DELETE - Neighborhoods','DELETE - Police Districts',
                            'DELETE - Supervisor Districts','DELETE - Fire Prevention Districts','DELETE - Zip Codes','CBD, BID and GBD Boundaries as of 2017',
                            'Central Market/Tenderloin Boundary','Areas of Vulnerability, 2016','Central Market/Tenderloin Boundary Polygon - Updated',
                            'Fix It Zones as of 2018-02-07','Neighborhoods'], axis=1,inplace=True)
        
        """
        Changes all the boolen from True/False to 1 and 0
        """
        new_data_frame['Non Payment']=new_data_frame['Non Payment'].apply(lambda x:boolens(x))
        new_data_frame['Breach']=new_data_frame['Breach'].apply(lambda x:boolens(x))
        new_data_frame['Nuisance']=new_data_frame['Nuisance'].apply(lambda x:boolens(x))
        new_data_frame['Illegal Use']=new_data_frame['Illegal Use'].apply(lambda x:boolens(x))
        new_data_frame['Failure to Sign Renewal']=new_data_frame['Failure to Sign Renewal'].apply(lambda x:boolens(x))
        new_data_frame['Access Denial']=new_data_frame['Access Denial'].apply(lambda x:boolens(x))
        new_data_frame['Unapproved Subtenant']=new_data_frame['Unapproved Subtenant'].apply(lambda x:boolens(x))
        new_data_frame['Owner Move In']=new_data_frame['Owner Move In'].apply(lambda x:boolens(x))
        new_data_frame['Demolition']=new_data_frame['Demolition'].apply(lambda x:boolens(x))
        new_data_frame['Capital Improvement']=new_data_frame['Capital Improvement'].apply(lambda x:boolens(x))
        new_data_frame['Demolition']=new_data_frame['Demolition'].apply(lambda x:boolens(x))
        new_data_frame['Substantial Rehab']=new_data_frame['Substantial Rehab'].apply(lambda x:boolens(x))
        new_data_frame['Ellis Act WithDrawal']=new_data_frame['Ellis Act WithDrawal'].apply(lambda x:boolens(x))
        new_data_frame['Condo Conversion']=new_data_frame['Condo Conversion'].apply(lambda x:boolens(x))
        new_data_frame['Roommate Same Unit']=new_data_frame['Roommate Same Unit'].apply(lambda x:boolens(x))
        new_data_frame['Other Cause']=new_data_frame['Other Cause'].apply(lambda x:boolens(x))
        new_data_frame['Late Payments']=new_data_frame['Late Payments'].apply(lambda x:boolens(x))
        new_data_frame['Lead Remediation']=new_data_frame['Lead Remediation'].apply(lambda x:boolens(x))
        new_data_frame['Development']=new_data_frame['Development'].apply(lambda x:boolens(x))
        new_data_frame['Good Samaritan Ends']=new_data_frame['Good Samaritan Ends'].apply(lambda x:boolens(x))

        # Clean and standardize the ZIP code column:
        new_data_frame['Eviction Notice Source Zipcode'] = new_data_frame['Eviction Notice Source Zipcode'].apply(clean_zip)

        # Replace empty strings with NaN temporarily
        new_data_frame['Eviction Notice Source Zipcode'] = new_data_frame['Eviction Notice Source Zipcode'].replace("", pd.NA)
        
        #fill NaN with mode
        new_data_frame['Eviction Notice Source Zipcode']=new_data_frame.groupby('Owner Move In')['Eviction Notice Source Zipcode'].transform(lambda x:x.fillna(x.mode()[0]))

        #cleaning the state cloumn to make it standadized 'CA'
        new_data_frame['State']=new_data_frame['State'].apply(lambda x:state(x))

        #cleaning the city name to have San Francisco
        new_data_frame['City']=new_data_frame['City'].apply(lambda x:clean_city(x))

        #clean Location: filling NaN by the mode
        new_data_frame['Location']=new_data_frame.groupby('File Date')['Location'].transform(lambda x:x.fillna(x.mode( )[0]))

        #clean Shape: filling NaN
        new_data_frame['Shape']=new_data_frame.groupby('Location')['Shape'].transform(lambda x:x.fillna(x.mode( )[0]))

        #clean Constraints Date:filling NaN
        new_data_frame['Constraints Date']=new_data_frame.groupby('Owner Move In')['Constraints Date'].transform(lambda x:x.fillna(x.mode()[0]))

        #extracting the latitude and longitude from location
        new_data_frame[['Location_Latitude', 'Location_Longitude']] = new_data_frame['Location'].apply(extract_lat_lon)

        #extracting the latitude and longitude from shape
        new_data_frame[['Shape_Latitude', 'Shape_Longitude']] = new_data_frame['Shape'].apply(extract_lat_lon_from_shape)

        #drop original location columnn
        new_data_frame.drop(columns=['Location'], inplace=True)

        #drop original shape columnn
        new_data_frame.drop(columns=['Shape'], inplace=True)

        task_logger.info(new_data_frame)
        task_logger.info(f"Transformation is completed succesfully and ready to be loaded")
       

        return new_data_frame
        
    


    Initiaze_DB=database_initialization()
    extraction=extract()
    transformation=transform(extraction)
workflow()
