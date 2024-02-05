"""
A script to test the connection the local PostgreSQL database.
"""

import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

def define_conn():
    """
    Define a connection to the database.
    Here, we are using a local postgresql database.
    """
    # params
    load_dotenv()  # take environment variables from .env.

    host = "localhost"
    port = 5432
    db = "evan"
    user = os.environ.get("USER")
    pw = os.environ.get("PASSWORD")

    # Establish the connection; need to use SQLAlchemy since we will upload data
    # https://stackoverflow.com/questions/58203973/pandas-unable-to-write-to-postgres-db-throws-keyerror-select-name-from-sqlit
    # conn = psycopg2.connect(**params)

    engine = create_engine(f'postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}')
    conn = engine.connect()

    return engine, conn

try:
    define_conn()
    #print("Connected successfully!")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL:", error)
