# Import packages for environment configuration and database connection

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from schemas.schema import Base

load_dotenv()

ENVIRONMENT=os.environ['ENVIRONMENT']

#development connection
if ENVIRONMENT=='development':

    DRIVERNAME=os.environ['DRIVERNAME']
    PASSWORD=os.environ['POSTGRES_PASSWORD']
    USER=os.environ['POSTGRES_USER']
    DB=os.environ['POSTGRES_DB']
    HOST=os.environ['HOST']
    PORT=os.environ['AWS_POSTGRES_PORT']


#RDS production connection
else:
    DRIVERNAME=os.environ['DRIVERNAME']
    HOST=os.environ['AWS_POSTGRES_HOST']
    PORT=os.environ['AWS_POSTGRES_PORT']
    USER=os.environ['AWS_POSTGRES_USER']
    PASSWORD=os.environ['AWS_POSTGRES_PASSWORD']
    DB=os.environ['AWS_POSTGRES_DB']

#database configuration
database_configuration=URL.create(drivername=DRIVERNAME,
                                  username=USER, 
                                  password=PASSWORD,
                                  host=HOST,
                                  database=DB
                                  )


#start the engine
engine=create_engine(database_configuration)

#database initialization
def database_initialize():
    return Base.metadata.create_all(engine)
                                  
