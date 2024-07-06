import pandas as pd
import json
import glob
import logging
from datetime import datetime

# Set up logging
log_path = "/opt/airflow/logs/data_processing.log"
logging.basicConfig(filename=log_path, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def process_json_files(json_path,output_parquet_file):
    json_files = glob.glob(json_path)
    dfs = []

    for file in json_files:
        try:
            logging.info(f"Processing file: {file}")
            with open(file, 'r') as f:
                data = json.load(f)
                df = pd.json_normalize(data, 'reporting_structure',
                                       ['reporting_entity_name', 'reporting_entity_type'],
                                       record_prefix='structure_')

                # Explode arrays
                df = df.explode('structure_reporting_plans').reset_index(drop=True)
                df = pd.concat(
                    [df.drop(['structure_reporting_plans'], axis=1), df['structure_reporting_plans'].apply(pd.Series)],
                    axis=1)

                if 'structure_in_network_files' in df.columns:
                    df = df.explode('structure_in_network_files').reset_index(drop=True)
                    df = pd.concat([df.drop(['structure_in_network_files'], axis=1),
                                    df['structure_in_network_files'].apply(pd.Series)], axis=1)
                elif 'structure_in_amount_files' in df.columns:
                    df = df.explode('structure_in_amount_files').reset_index(drop=True)
                    df = pd.concat([df.drop(['structure_in_amount_files'], axis=1),
                                    df['structure_in_amount_files'].apply(pd.Series)], axis=1)

                # Append the processed DataFrame to the list
                dfs.append(df)
        except Exception as e:
            logging.error(f"Error processing file {file}: {e}")

    # Concatenate all the DataFrames
    final_df = pd.concat(dfs, ignore_index=True)

    # Select and reorder columns
    final_df = final_df[
        ['reporting_entity_name', 'reporting_entity_type', 'plan_name', 'plan_id', 'plan_id_type', 'plan_market_type',
         'description', 'location']]

    # Convert plan_id to numeric with errors='raise'
    try:
        final_df['plan_id'] = pd.to_numeric(final_df['plan_id'], errors='raise').astype('Int64')
    except Exception as e:
        logging.error(f"Error converting plan_id to numeric: {e}")
        raise
    try:
        final_df.to_parquet(output_parquet_file)
        logging.info(f"Saved processed data to Parquet file: {output_parquet_file}")
        print(f"Data stored as Parquet file: {output_parquet_file}")
    except Exception as e:
        logging.error(f"Error saving data to Parquet file: {e}")
        raise

    return final_df


if __name__ == "__main__":

    json_path = "/opt/airflow/data/raw/*.json"
    today_date = datetime.now().strftime('%Y%m%d')
    output_parquet_file = f"/opt/airflow/data/processed/processed_data_{today_date}.parquet"
    process_json_files(json_path, output_parquet_file)

