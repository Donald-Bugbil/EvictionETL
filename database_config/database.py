# Import packages for environment configuration and database connection

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from schemas.schema import Base

load_dotenv()
#database connection
DRIVERNAME=os.environ['DRIVERNAME']
# POSTGRES_PASSWORD=os.environ['POSTGRES_PASSWORD']
# POSTGRES_USER=os.environ['POSTGRES_USER']
# POSTGRES_DB=os.environ['POSTGRES_DB']
# HOST=os.environ['HOST']

#RDS connection
AWS_POSTGRES_HOST=os.environ['AWS_POSTGRES_HOST']
AWS_POSTGRES_PORT=os.environ['AWS_POSTGRES_PORT']
AWS_POSTGRES_USER=os.environ['AWS_POSTGRES_USER']
AWS_POSTGRES_PASSWORD=os.environ['AWS_POSTGRES_PASSWORD']
AWS_POSTGRES_DB=os.environ['AWS_POSTGRES_DB']

# #database configuration
# database_configuration=URL.create(drivername=DRIVERNAME,
#                                   username=POSTGRES_USER, 
#                                   password=POSTGRES_PASSWORD,
#                                   host=HOST,
#                                   database=POSTGRES_DB
#                                   )

database_configuration=URL.create(drivername=DRIVERNAME,
                                  username=AWS_POSTGRES_USER, 
                                  password=AWS_POSTGRES_PASSWORD,
                                  host=AWS_POSTGRES_HOST,
                                  port=AWS_POSTGRES_PORT,
                                  database=AWS_POSTGRES_DB
                                  )

#start the engine
engine=create_engine(database_configuration)

#database initialization
def database_initialize():
    return Base.metadata.create_all(engine)
                                  
