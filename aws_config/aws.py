# Import packages for environment configuration 

import os
from dotenv import load_dotenv
load_dotenv()

SECRET_KEY=os.environ['SECRET_KEY']
ACCESS_KEY=os.environ['ACCESS_KEY']
BUCKET_NAME=os.environ['BUCKET_NAME']