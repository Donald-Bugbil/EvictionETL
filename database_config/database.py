# Import packages for environment configuration and database connection

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from schemas.schema import Base

load_dotenv()
#database connection
DRIVERNAME=os.environ['DRIVERNAME']
POSTGRES_PASSWORD=os.environ['POSTGRES_PASSWORD']
POSTGRES_USER=os.environ['POSTGRES_USER']
POSTGRES_DB=os.environ['POSTGRES_DB']
HOST=os.environ['HOST']

#database configuration
database_configuration=URL.create(drivername=DRIVERNAME,
                                  username=POSTGRES_USER, 
                                  password=POSTGRES_PASSWORD,
                                  host=HOST,
                                  database=POSTGRES_DB
                                  )

#start the engine
engine=create_engine(database_configuration)

#database initialization
def database_initialize():
    return Base.metadata.create_all(engine)
                                  
