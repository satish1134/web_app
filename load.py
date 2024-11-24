import psycopg2
import logging
import os
import pandas as pd
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from datetime import datetime

#Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load secret key
with open('secret.key', 'rb') as key_file:
    key = key_file.read()

# Initialize Fernet with the secret key
fernet = Fernet(key)

# Decrypt environment variables
def decrypt_env_var(encrypted_value):
    try:
        return fernet.decrypt(encrypted_value.encode()).decode()
    except Exception as e:
        logger.error("Failed to decrypt environment variable: %s", e)
        return None

def connect_to_db():
    try:
        # Decrypt each credential from .env
        dbname = decrypt_env_var(os.getenv('DATABASE'))
        user = decrypt_env_var(os.getenv('USER'))
        password = decrypt_env_var(os.getenv('PASSWORD'))
        host = decrypt_env_var(os.getenv('HOST'))
        port = decrypt_env_var(os.getenv('PORT'))

        # Establish connection
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logger.info("Connection to the database established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error("Connection to the database failed: %s", e)
        return None

def create_table_if_not_exists(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dag_data (
                SUBJECT_AREA VARCHAR(50),
                DAG_NAME VARCHAR(50),
                STATUS VARCHAR(20),
                DAG_START_TIME TIMESTAMP,
                DAG_END_TIME TIMESTAMP,
                ELAPSED_TIME INTERVAL,
                MODIFIED_TS TIMESTAMP
            );
        """)
        connection.commit()
        cursor.close()
        logger.info("Table 'dag_data' checked and created if not exists.")
    except Exception as e:
        logger.error("Error creating table: %s", e)

def truncate_table(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE dag_data;")
        connection.commit()
        cursor.close()
        logger.info("Table 'dag_data' truncated successfully.")
    except Exception as e:
        logger.error("Error truncating table: %s", e)

def load_data_to_db(connection, csv_file):
    try:
        # Load CSV data into a DataFrame
        df = pd.read_csv(csv_file)

        # Convert the date columns to datetime
        df['DAG_START_TIME'] = pd.to_datetime(df['DAG_START_TIME'], format='%d-%m-%Y %H:%M', errors='coerce')
        df['DAG_END_TIME'] = pd.to_datetime(df['DAG_END_TIME'], format='%d-%m-%Y %H:%M', errors='coerce')

        # Calculate ELAPSED_TIME as an interval, allowing for missing DAG_END_TIME
        df['ELAPSED_TIME'] = df.apply(
            lambda row: (row['DAG_END_TIME'] - row['DAG_START_TIME']).total_seconds() if pd.notnull(row['DAG_END_TIME']) else None, axis=1
        )

        # Add MODIFIED_TS with the current timestamp
        df['MODIFIED_TS'] = datetime.now()

        # Log the DataFrame before insertion for debugging
        logger.info("DataFrame ready for insertion:\n%s", df)

        # Prepare data for insertion and handle missing end time
        cursor = connection.cursor()
        for _, row in df.iterrows():
            if pd.isnull(row['DAG_START_TIME']):
                logger.warning(f"Skipping row due to invalid start time: {row.to_dict()}")
                continue  # Skip rows with NaT for start time

            try:
                # Convert None values to NULL for insertion
                dag_end_time = row['DAG_END_TIME'].to_pydatetime() if pd.notnull(row['DAG_END_TIME']) else None
                elapsed_time = row['ELAPSED_TIME'] if pd.notnull(row['ELAPSED_TIME']) else None

                # Convert elapsed_time from seconds to INTERVAL for PostgreSQL
                elapsed_time_interval = f"{int(elapsed_time)} seconds" if elapsed_time is not None else None

                # Log the values being inserted for debugging
                logger.info(f"Inserting row: {row['SUBJECT_AREA']}, {row['DAG_NAME']}, {row['STATUS']}, {row['DAG_START_TIME']}, {dag_end_time}, {elapsed_time_interval}, {row['MODIFIED_TS']}")

                cursor.execute("""
                    INSERT INTO dag_data (SUBJECT_AREA, DAG_NAME, STATUS, DAG_START_TIME, DAG_END_TIME, ELAPSED_TIME, MODIFIED_TS)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['SUBJECT_AREA'],
                    row['DAG_NAME'],
                    row['STATUS'],
                    row['DAG_START_TIME'],
                    dag_end_time,
                    elapsed_time_interval,
                    row['MODIFIED_TS']
                ))

                # Commit after each successful insert
                connection.commit()
                
            except Exception as inner_e:
                logger.error(f"Failed to insert row {row.to_dict()}: {inner_e}")
                connection.rollback()
                continue  # Skip to the next row

        cursor.close()
        logger.info("Data loaded successfully into the database.")
    except Exception as e:
        logger.error("Error loading data into database: %s", e)

# Example usage
if __name__ == "__main__":
    connection = connect_to_db()
    if connection:
        create_table_if_not_exists(connection)  # Ensure table exists
        truncate_table(connection)  # Truncate the table before loading new data
        load_data_to_db(connection, 'C:/Users/satis/acad/test_file.csv')  # Specify the path to your CSV file
        connection.close()
    else:
        logger.warning("No connection established; exiting script.")
