import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
import pyarrow.parquet as pq
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(filename='load_parquet_to_postgres.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Configuration
DB_CONN_STRING = 'postgresql+psycopg2://airflow:airflow@localhost:5432/airflow'
PARQUET_FILE = f"/opt/airflow/data/processed/processed_data_{today_date}.parquet"
#PARQUET_FILE = '/Users/varshini/Downloads/Shafik/Data_Engineering/Airflow_Project/data/processed/processed_data.parquet'
# Create SQLAlchemy engine and metadata
engine = create_engine(DB_CONN_STRING)

# Read Parquet file into DataFrame
df = pq.read_table(PARQUET_FILE).to_pandas()
df['ingestion_timestamp'] = datetime.utcnow()

# Log and print the count of records being loaded
record_count = len(df)
logging.info(f"Number of records to be loaded: {record_count}")
print(f"Number of records to be loaded: {record_count}")

# Define the table schema to match the actual PostgreSQL table
metadata = MetaData(schema='health_plans')
uhg_plans = Table('uhg_plans', metadata,
                  Column('reporting_entity_name', String),
                  Column('reporting_entity_type', String),
                  Column('plan_name', String),
                  Column('plan_id', Integer),
                  Column('plan_id_type', String),
                  Column('plan_market_type', String),
                  Column('description', String),
                  Column('location', String),
                  Column('ingestion_timestamp', DateTime, default=datetime.utcnow))

load_batches = Table('load_batches', metadata,
                     Column('batch_id', Integer, primary_key=True, autoincrement=True),
                     Column('load_timestamp', DateTime, default=datetime.utcnow),
                     Column('record_count', Integer),
                     Column('status', String),
                     Column('main_table', String))
df['ingestion_timestamp'] = datetime.utcnow()

# Log and print the count of records being loaded
record_count = len(df)
logging.info(f"Number of records to be loaded: {record_count}")
print(f"Number of records to be loaded: {record_count}")

# Create a session and load data into PostgreSQL
Session = sessionmaker(bind=engine)
with Session() as session:
    try:
        # Insert data into the main table
        session.execute(uhg_plans.insert(), df.to_dict(orient='records'))
        session.commit()

        # Log the successful load and insert into the batch table
        logging.info(f"Successfully loaded {record_count} records from {PARQUET_FILE} to PostgreSQL table health_plans.uhg_plans")
        print(f"Successfully loaded {record_count} records into the table health_plans.uhg_plans")

        # Insert a record into the batch table
        batch_record = {
            'record_count': record_count,
            'status': 'Success',
            'main_table': 'health_plans.uhg_plans'
        }
        session.execute(load_batches.insert(), batch_record)
        session.commit()
    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {e}")
        print("Error occurred:", e)
        session.rollback()

        # Insert a record into the batch table with failure status
        batch_record = {
            'record_count': 0,
            'status': 'Failed',
            'main_table': 'health_plans.uhg_plans'
        }
        session.execute(load_batches.insert(), batch_record)
        session.commit()
