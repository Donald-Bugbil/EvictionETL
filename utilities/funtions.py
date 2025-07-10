import pandas as pd
import s3fs
from aws_config.aws import ACCESS_KEY, SECRET_KEY, BUCKET_NAME
from schemas.schema import Base, Eviction
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.models import Connection
from airflow import settings
import json
import logging
import boto3
from airflow.exceptions import AirflowException
from airflow.models import Variable

import boto3


import redshift_connector

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

    #construct an s3 file path
    file_path = f's3://{bucket_name}/{key}'
    
    #open s3 file and write into the file with the dataframes data
    with s3.open(file_path, 'w') as file:

        transform_dataframe.to_csv(file, index=False, header=True)



    #s3://etl/eviction_data.csv

    return file_path


# def create_redshift_table():
#     try:

#         aws_conn = Connection(
#             conn_id='aws_conn_a',
#             conn_type='aws',
#             login=ACCESS_KEY,
#             password=SECRET_KEY,

#             extra=json.dumps({
#                 "region_name": "us-east-2",
#             })
#         )

#         redshift_conn = Connection(
#             conn_id='redshift_conn_a',
#             conn_type='redshift',
#             host='default-workgroup.832928244233.us-east-2.redshift-serverless.amazonaws.com',
#             schema='dev',
#             port=5439,
#             extra=json.dumps({
#                 'iam': True,
#                 'is_serverless': True,  # This is crucial for serverless
#                 'serverless_work_group': 'default-workgroup',  # Use this instead of workgroup_name
#                 'db_user': 'kakeibo',
#                 'region_name': 'us-east-2',
#                 'aws_conn_id': 'aws_conn_a',  # Ensure this matches your AWS connection ID
#             })
#         )


#         session = settings.Session()

#         session.merge(redshift_conn)
#         session.merge(aws_conn)
#         session.commit()
#         session.close()


#         hook = RedshiftSQLHook(
#             redshift_conn_id='redshift_conn_a'
#         )

#         engine = hook.get_sqlalchemy_engine()

#         Base.metadata.create_all(engine, checkfirst=True)
#     except Exception as e:
#         raise Exception(f"Error creating Redshift table: {e}")

# def create_redshift_table():
#     try:
#         # Create AWS Connection object
#         aws_conn = Connection(
#             conn_id='aws_conn_i',
#             conn_type='aws',
#             login=ACCESS_KEY,  # Your AWS Access Key ID
#             password=SECRET_KEY,  # Your AWS Secret Access Key
#             extra=json.dumps({
#                 "region_name": "us-east-2"
#             })
#         )

#         # Create Redshift Serverless Connection object
#         redshift_conn = Connection(
#             conn_id='redshift_conn_i',
#             conn_type='redshift',
#             host='default-workgroup.832928244233.us-east-2.redshift-serverless.amazonaws.com',
#             schema='dev',
#             port=5439,
#             extra=json.dumps({
#                 'iam': True,
#                 'is_serverless': True,
#                 'serverless_work_group': 'default-workgroup',
#                 'db_user': 'kakeibo', # Explicitly specify database name for Redshift Serverless
                
#             })
#         )

#         # Save connections to Airflow metadata database
#         session = settings.Session()
#         session.merge(redshift_conn)
#         session.merge(aws_conn)
#         session.commit()
#         session.close()

#         # Use RedshiftSQLHook with both connection IDs
#         hook = RedshiftSQLHook(
#             redshift_conn_id='redshift_conn_i',
#             aws_conn_id='aws_conn_i'  # Explicitly pass aws_conn_id
#         )

      
#         engine = hook.get_sqlalchemy_engine()
#         Base.metadata.create_all(engine, checkfirst=True)
#         logging.info("Table creation completed")
#     except Exception as e:
#         logging.error("Error creating Redshift table: %s", e)
#         raise Exception(f"Error creating Redshift table: {e}")


def create_redshift_table():

    try:

        session = boto3.Session()

        logging.info("boto3 detected region: %s", session.region_name)

        # Create Redshift Serverless Connection object
        redshift_conn = Connection(
            conn_id='redshift_conn_tester',
            conn_type='redshift',
            host='default-workgroup.832928244233.us-east-2.redshift-serverless.amazonaws.com',
            schema='dev',
            port=5439,
            extra={
                'iam': True,
                'is_serverless': True,
                'serverless_work_group': 'default-workgroup',
                'db_user': 'kakeibo',  # Fixed typo from 'kakibo'
            }
        )

        # Save connections to Airflow metadata database
        session = settings.Session()
        session.merge(redshift_conn)
        session.commit()
        session.close()

        # Use RedshiftSQLHook with both connection IDs
        hook = RedshiftSQLHook(
            redshift_conn_id='redshift_conn_tester',
        )

        # Create table using SQLAlchemy engine
        engine = hook.get_sqlalchemy_engine()
        Base.metadata.create_all(engine, checkfirst=True)
        logging.info("Table creation completed")
    except Exception as e:
        logging.error("Error creating Redshift table: %s", e)
        raise Exception(f"Error creating Redshift table: {e}")

    # conn = redshift_connector.connect(
    #     host='default-workgroup.832928244233.us-east-2.redshift-serverless.amazonaws.com',
    #     database='dev',
    #     port=5439,
    #     db_user='kakibo',
    #     iam=True,
    #     is_serverless=True,
    #     serverless_work_group='default-workgroup',
    #     access_key_id=ACCESS_KEY,
    #     secret_access_key=SECRET_KEY,
    #     region='us-east-2'
    #     )

    # with conn.cursor() as cursor:
    #     cursor.execute(
    #         """
    #         CREATE TABLE IF NOT EXISTS eviction (
    #             id INT IDENTITY(1,1) PRIMARY KEY,
    #             eviction_id VARCHAR(255),
    #             address VARCHAR(255),
    #             city VARCHAR(255),
    #             state VARCHAR(50),
    #             eviction_notice_zipcode INT,
    #             file_date DATE,
    #             non_payment INT,
    #             breach INT,
    #             nuisance INT,
    #             illegal_use INT,
    #             failure_to_sign_renewal INT,
    #             access_denial INT,
    #             unapproved_subtenant INT,
    #             owner_move_in INT,
    #             demolition INT,
    #             capital_improvement INT,
    #             substantial_rehab INT,
    #             ellis_act_withdrawal INT,
    #             condo_conversion INT,
    #             roomate_same_unit INT,
    #             other_cause INT,
    #             late_payments INT,
    #             lead_remediation INT,
    #             development INT,
    #             good_samaritan_ends INT,
    #             constraints_date DATE,
    #             data_as_of DATE,
    #             data_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #             location_latitude FLOAT8,
    #             location_longitude FLOAT8,
    #             shape_latitude FLOAT8,
    #             shape_longitude FLOAT8
    #         )
    #         """
    #     )
    #     conn.commit()

    
def load_to_redshift(s3_path, table_name):

    hook = RedshiftSQLHook(
        redshift_conn_id='redshift_default',
        aws_conn_id='aws_default'
    )

    def test():
        pass
    hook.get_cursor().execute(
        f"""
        COPY {table_name}
        FROM '{s3_path}'
        CSV
        IGNOREHEADER 1
        """
    )
    
    




