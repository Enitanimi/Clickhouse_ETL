

import sqlalchemy
from sqlalchemy import create_engine
import clickhouse_connect
from dotenv import load_dotenv
import os

# create function to get the clickhouse client connect

load_dotenv(override=True)

def get_client():
    '''
    Connects to a clickhouse database using parameters from a .env file

    Parameters: None

    Returns:
    -  clickhouse_connect.client: A database client object
    '''

    ## getting the credentials

    host = os.getenv("ch_host")
    port = os.getenv("ch_port")
    user = os.getenv("ch_user")
    password = os.getenv("ch_pword")

    ## connect to database

    client = clickhouse_connect.get_client(host=host, port=port, username=user, password=password, secure=True)

    return client

# create database connection

def get_postgres_engine():
    '''
    Constructs a SQLalchemy engine object for postgres DB from .env file

    Parameters: None

    Returns:

    - sqlachemy engine (sqlalchemy.engine.Engine)    
    '''


    engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
                            user = os.getenv("pg_user"),
                            password = os.getenv("pg_pword"),
                            host = os.getenv("pg_host"),
                            port = os.getenv("pg_port"),
                            dbname = os.getenv("pg_DB")
                                )
                        )

    return engine

# create function to get postgres connect
