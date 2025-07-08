import pandas as pd
import s3fs
from aws_config.aws import ACCESS_KEY, SECRET_KEY, BUCKET_NAME
from schemas.schema import Base, Eviction
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

import boto3
from airflow.exceptions import AirflowException

import redshift_connector


# session = boto3.Session(
#     aws_access_key_id= ACCESS_KEY,
#     aws_secret_access_key= SECRET_KEY,
#     region_name='us-east-2'
# )

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
def upload_to_s3(transform_dataframe: pd.DataFrame, bucket_name, key):
    s3 = s3fs.S3FileSystem(anon=False, secret=SECRET_KEY, key=ACCESS_KEY)
    file_path = f's3://{bucket_name}/{key}'
    #convert dataframe to csv
    transform_dataframe.to_csv(file_path, index=False, header=True)

    #s3://etl/eviction_data.csv

    return file_path


def create_redshift_table():
    try:

        hook = RedshiftSQLHook(
            redshift_conn_id='redshift_default'
        )

        engine = hook.get_sqlalchemy_engine()

        Base.metadata.create_all(engine, checkfirst=True)
    except Exception as e:
        raise Exception(f"Error creating Redshift table: {e}")
    
    finally:
        engine.dispose()
    


    
def load_to_redshift(s3_path, table_name):

    hook = RedshiftSQLHook(
        redshift_conn_id='redshift_default'
    )

    hook.get_cursor().execute(
        f"""
        COPY {table_name}
        FROM '{s3_path}'
        IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftCopyRole'
        CSV
        IGNOREHEADER 1
        """
    )
    
    




