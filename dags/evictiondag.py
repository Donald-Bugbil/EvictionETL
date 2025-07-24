# Import necessary packages 

import pandas as pd
import pendulum
from airflow.decorators import task, dag
import logging
from database_config.database import engine, database_initialize
from Clients.client import get_redshift_client, get_s3_client
from aws_config.aws import  BUCKET_NAME
from sqlalchemy.orm import Session
from schemas.schema import Eviction
from utilities.funtions import boolens, clean_zip, state, clean_city, column_names, extract_lat_lon, extract_lat_lon_from_shape, upload_to_s3, create_redshift_table, load_to_redshift
from schemas.schema import Eviction
import io


#task logger
task_logger=logging.getLogger('workflow.task')

# table_name = 'eviction'

# new_data_frame = ''



#  id=Column(Integer, primary_key=True, autoincrement=True)
#     eviction_id=Column(String)
#     address=Column(String)
#     city=Column(String)
#     state=Column(String)
#     eviction_notice_zipcode=Column(Integer)
#     file_date=Column(DateTime)
#     non_payment=Column(Integer)
#     breach=Column(Integer)
#     nuisance=Column(Integer)
#     illegal_use=Column(Integer)
#     failure_to_sign_renewal=Column(Integer)
#     access_denial=Column(Integer)
#     unapproved_subtenant=Column(Integer)
#     owner_move_in=Column(Integer)
#     demolition=Column(Integer)
#     capital_improvement=Column(Integer)
#     substantial_rehab=Column(Integer)
#     ellis_act_withdrawal=Column(Integer)
#     condo_conversion=Column(Integer)
#     roomate_same_unit=Column(Integer)
#     other_cause=Column(Integer)
#     late_payments=Column(Integer)
#     lead_remediation=Column(Integer)
#     development=Column(Integer)
#     good_samaritan_ends=Column(Integer)
#     constraints_date=Column(DateTime)
#     data_as_of=Column(DateTime)
#     data_loaded_at=Column(DateTime)
#     location_latitude=Column(Float)
#     location_longitude=Column(Float)
#     shape_latitude=Column(Float)
#     shape_longitude=Column(Float)


# #['Eviction ID', 'Address', 'City', 'State',
#        'Eviction Notice Source Zipcode', 'File Date', 'Non Payment', 'Breach',
#        'Nuisance', 'Illegal Use', 'Failure to Sign Renewal', 'Access Denial',
#        'Unapproved Subtenant', 'Owner Move In', 'Demolition',
#        'Capital Improvement', 'Substantial Rehab', 'Ellis Act WithDrawal',
#        'Condo Conversion', 'Roommate Same Unit', 'Other Cause',
#        'Late Payments', 'Lead Remediation', 'Development',
#        'Good Samaritan Ends', 'Constraints Date', 'data_as_of',
#        'data_loaded_at', 'Location_Latitude', 'Location_Longitude',
#        'Shape_Latitude', 'Shape_Longitude'],
#       dtype='object')


#dag
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
        task_logger.info(f"DataFrame columns: {data_frame.columns.tolist()}")

        return data_frame
    
    
    #This task cleans and transformed the data to be loaded into the DB
    @task()
    def transform(data_frame):
        new_data_frame: pd.DataFrame = data_frame

        string_columms = ['eviction_id', 'address', 'city', 'state']

        boolean_columns = ['non_payment', 'breach', 'nuisance', 'illegal_use', 'failure_to_sign_renewal',
                           'access_denial', 'unapproved_subtenant', 'owner_move_in', 'demolition',
                           'capital_improvement', 'substantial_rehab', 'ellis_act_withdrawal',
                           'condo_conversion', 'roommate_same_unit', 'other_cause', 'late_payments',
                           'lead_remediation', 'development', 'good_samaritan_ends',  'eviction_notice_zipcode']
        
        # date_columns = ['file_date', 'constraints_date', 'data_as_of', 'data_loaded_at']
        date_columns = ['file_date', 'constraints_date'] 
        date_time_columns = ['data_as_of', 'data_loaded_at']

        float_columns = ['location_latitude', 'location_longitude', 'shape_latitude', 'shape_longitude']


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

        #drop null in Address column which doesn't impact decision
        new_data_frame.dropna(subset=['Address'], inplace=True)

        task_logger.info(new_data_frame)
        task_logger.info(f"Transformation is completed succesfully and ready to be loaded")
        task_logger.info(f"DataFrame columns: {new_data_frame.columns.tolist()}")

        date_format = '%m/%d/%Y'
        date_time_format = '%m/%d/%Y %H:%M'

        new_data_frame.rename(columns=column_names, inplace=True)

        #convert all string columns to string type
        for col in string_columms:
            new_data_frame[col] = new_data_frame[col].astype(str)
        
        #convert all float columns to float types
        for col in float_columns:
            new_data_frame[col] = pd.to_numeric(new_data_frame[col], errors='coerce')

        #convert all date columns to date types
        for col in date_columns:
            new_data_frame[col] = pd.to_datetime(new_data_frame[col], errors='coerce', format=date_format)
        
        #convert all date time columns to date time types
        for col in date_time_columns:
            new_data_frame[col] = pd.to_datetime(new_data_frame[col], format='mixed')
        #new_data_frame['data_loaded_at'] = pd.to_datetime('now').strftime('%Y-%m-%d %H:%M:%S')

        #convert all boolean columns to boolean types
        for col in boolean_columns:
            new_data_frame[col] = new_data_frame[col].astype(int)
        
        #drop an unknown date in the file_date column
        new_data_frame.dropna(subset=['file_date', 'data_loaded_at', 'data_as_of', 'constraints_date'], inplace=True)


        logging.info(f"Transformed DataFrame columns: {new_data_frame.columns.tolist()}")
        logging.info(f"Transformed DataFrame info: {new_data_frame.info()}")
       

        # return new_data_frame

        return new_data_frame
    

    @task()
    def load(transform_data, database_state):
        list_objects = []
        def create_objects(row):

            new_object = Eviction(
                eviction_id= row['eviction_id'],
                address=row['address'],
                city=row['city'],
                state=row['state'],
                eviction_notice_zipcode=row['eviction_notice_zipcode'],
                file_date=row['file_date'],
                non_payment=row['non_payment'],
                breach=row['breach'],
                nuisance=row['nuisance'],
                illegal_use=row['illegal_use'],
                failure_to_sign_renewal=row['failure_to_sign_renewal'],
                access_denial=row['access_denial'],
                unapproved_subtenant=row['unapproved_subtenant'],
                owner_move_in=row['owner_move_in'],
                demolition=row['demolition'],
                capital_improvement=row['capital_improvement'],
                substantial_rehab=row['substantial_rehab'],
                ellis_act_withdrawal=row['ellis_act_withdrawal'],
                condo_conversion=row['condo_conversion'],
                roommate_same_unit=row['roommate_same_unit'],
                other_cause=row['other_cause'],
                late_payments=row['late_payments'],
                lead_remediation=row['lead_remediation'],
                development=row['development'],
                good_samaritan_ends=row['good_samaritan_ends'],
                constraints_date=row['constraints_date'],
                data_as_of=row['data_as_of'],
                data_loaded_at=row['data_loaded_at'],
                location_latitude=row['location_latitude'],
                location_longitude=row['location_longitude'],
                shape_latitude=row['shape_latitude'],
                shape_longitude=row['shape_longitude']

            )

            list_objects.append(new_object)


        if database_state is True:
            data_to_load=transform_data
            task_logger.info(data_to_load)
            task_logger.info(f'data is  ready to load')  
            data_to_load.apply(lambda row: create_objects(row), axis=1)     
                                   
            with Session(engine) as session:
                session.add_all(list_objects)
                session.commit()
                task_logger.info(f'data loaded successfully')
                return "load complete"

        else:
            task_logger.warning(f'database not initialized skipping loading')
            return "Skipped load due to database error"




    Initiaze_DB=database_initialization()
    Extraction=extract()
    Transformation=transform(Extraction)
    load(Transformation, Initiaze_DB)
workflow()



