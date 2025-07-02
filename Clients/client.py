#establishing client with s3 and redshift
import boto3
from aws_config.aws import ACCESS_KEY, SECRET_KEY, BUCKET_NAME

session=boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def get_s3_client():
    s3_client=session.client('s3')
    return s3_client

def get_redshift_client():
    redshift_client=session.client('redshift')
    return redshift_client