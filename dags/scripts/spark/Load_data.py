import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
import pyarrow.parquet as pq
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(filename='load_parquet_to_postgres.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Configuration
DB_CONN_STRING = 'postgresql+psycopg2://airflow:airflow@localhost:5432/United_health'
PARQUET_FILE = '/opt/airflow/data/processed/processed_data.parquet'

# Create SQLAlchemy engine and metadata
engine = create_engine(DB_CONN_STRING)

# Read Parquet file into DataFrame
df = pq.read_table(PARQUET_FILE).to_pandas()
df['ingestion_timestamp'] = datetime.utcnow()

# Log and print the count of records being loaded
record_count = len(df)
logging.info(f"Number of records to be loaded: {record_count}")
print(f"Number of records to be loaded: {record_count}")

# Create a session and load data into PostgreSQL
Session = sessionmaker(bind=engine)
with Session() as session:
    try:
        session.execute(uhg_plans.insert(), df.to_dict(orient='records'))
        session.commit()
        logging.info(f"Successfully loaded data from {PARQUET_FILE} to PostgreSQL table uhg_plans")
    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {e}")
        session.rollback()
