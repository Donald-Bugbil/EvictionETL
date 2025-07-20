import pandas as pd
import s3fs
from aws_config.aws import ACCESS_KEY, SECRET_KEY, BUCKET_NAME
from schemas.schema import Base, Eviction
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.models import Connection
from airflow import settings
import json
import logging

from airflow.exceptions import AirflowException
from airflow.models import Variable







#A function to convert all the boolen to 1 and 0 representingthe True/false respectively from the raw data
def boolens(value):
    if value is True:
        return 1
    else:
        return 0
    
#This function cleans the zipcode
def clean_zip(val):
    try:
        if pd.isnull(val):
            return ''
        
        val = int(float(val))  # Handles strings and floats like '94110.0'

        # San Francisco ZIP code range
        if 94102 <= val <= 94188:
            return f"{val:05d}"
        else:
            return ''  # Not a valid SF ZIP
    except:
        return ''  # Invalid or malformed input
    
#cleaning the State
def state(name):
    if pd.isna(name):
        return 'CA'
    name = name.strip()
    if name in ['Ca', 'CA`', 'nan']:
        return 'CA'
    return name

#cleaning City
def clean_city(name):
    if pd.isna(name):
        return 'San Francisco'
    name=name.strip()
    if name in ['San Franicsco','nan', 'San Frnaicsco','San Franisco','San Francicso','San Franciso','San Frnacisco' ,'San  Frnacisco',
                'San ‘francisco','C' ,'3/9/2017', 'San Frncisco', 'La Canada','San Francisoc' ,'Tom Francisco', 'Sn Francisco', 
                '399 Haight Street', '459 Turk Street', 'San Francisc', 'San Francisco`', '158an Francisco']:
        return 'San Francisco'
    return name

# Extract Latitude and Longitude from the 'Location' column    
def extract_lat_lon(location_str):
    try:
        # Remove parentheses and split
        lat, lon = location_str.strip("()").split(",")
        return pd.Series([float(lat), float(lon)])
    except:
        return pd.Series([None, None])  # Handle malformed entries
    
# Extract Latitude and Longitude from the 'Shape' column    
def extract_lat_lon_from_shape(shape_str):
    try:
        # Remove 'POINT (' and ')' and split
        lon, lat = shape_str.replace("POINT", "").strip(" ()").split()
        return pd.Series([float(lat), float(lon)])
    except:
        return pd.Series([None, None])

#is to save the transformed dataframe to csv and return the path
def upload_to_s3(transform_dataframe: pd.DataFrame,key, bucket_name):
    
    s3 = s3fs.S3FileSystem(anon=False, secret=SECRET_KEY, key=ACCESS_KEY)

    #construct an s3 file path
    file_path = f's3://{bucket_name}/{key}'
    
    #open s3 file and write into the file with the dataframes data
    with s3.open(file_path, 'w') as file:

        transform_dataframe.to_csv(file, index=False, header=True)

    return file_path


def create_redshift_table():

    try: 
        # Use RedshiftSQLHook with both connection IDs
        hook = RedshiftSQLHook(
            redshift_conn_id='redshift_default',
            aws_conn_id='aws_default'
        )

        # Create table using SQLAlchemy engine
        engine = hook.get_sqlalchemy_engine()
        Base.metadata.create_all(engine, checkfirst=True)
        logging.info("Table creation completed")
    except Exception as e:
        logging.error("Error creating Redshift table: %s", e)
        raise Exception(f"Error creating Redshift table: {e}")


    
def load_to_redshift(s3_path, table_name):

    hook = RedshiftSQLHook(
        redshift_conn_id='redshift_default',
        aws_conn_id='aws_default'
    )


    try:
   
        with hook.get_conn() as conn:

            cursor = conn.cursor()

            copy_query = f"""
                COPY {table_name} (

                eviction_id,         
                address,              
                city,                    
                state,                        
                eviction_notice_zipcode,       
                file_date,               
                non_payment,                  
                breach,                           
                nuisance,                         
                illegal_use,                      
                failure_to_sign_renewal,          
                access_denial,                    
                unapproved_subtenant,             
                owner_move_in,                    
                demolition,                    
                capital_improvement,             
                substantial_rehab,                 
                ellis_act_withdrawal,             
                condo_conversion,               
                roommate_same_unit,             
                other_cause,                     
                late_payments,                   
                lead_remediation,                 
                development,                  
                good_samaritan_ends,               
                constraints_date,         
                data_as_of,               
                data_loaded_at,           
                location_latitude,            
                location_longitude,             
                shape_latitude,               
                shape_longitude                     
                )
                FROM '{s3_path}'
                IAM_ROLE 'arn:aws:iam::832928244233:role/redshiftS3FullAccess'
                REGION 'us-east-2'
                FORMAT AS CSV
                DELIMITER ','
                IGNOREHEADER 1;
                """
            


            cursor.execute(copy_query)
            conn.commit()
    
    except Exception as e:
        raise Exception(f'Error loading data to redshift table {table_name}: {e}')


    
column_names = {
    'Eviction ID': 'eviction_id',
    'Address': 'address',
    'City': 'city',
    'State': 'state',
    'Eviction Notice Source Zipcode': 'eviction_notice_zipcode',
    'File Date': 'file_date',
    'Non Payment': 'non_payment',
    'Breach': 'breach',
    'Nuisance': 'nuisance',
    'Illegal Use': 'illegal_use',
    'Failure to Sign Renewal': 'failure_to_sign_renewal',
    'Access Denial': 'access_denial',
    'Unapproved Subtenant': 'unapproved_subtenant',
    'Owner Move In': 'owner_move_in',
    'Demolition': 'demolition',
    'Capital Improvement': 'capital_improvement',
    'Substantial Rehab': 'substantial_rehab',
    'Ellis Act WithDrawal': 'ellis_act_withdrawal',
    'Condo Conversion': 'condo_conversion',
    'Roommate Same Unit': 'roommate_same_unit',
    'Other Cause': 'other_cause',
    'Late Payments': 'late_payments',
    'Lead Remediation': 'lead_remediation',
    'Development': 'development',
    'Good Samaritan Ends': 'good_samaritan_ends',
    'Constraints Date': 'constraints_date',
    'data_as_of': 'data_as_of',
    'data_loaded_at': 'data_loaded_at',
    'Location_Latitude': 'location_latitude',
    'Location_Longitude': 'location_longitude',
    'Shape_Latitude': 'shape_latitude',
    'Shape_Longitude': 'shape_longitude'
    }

