import pandas as pd
import vertica_python
from cryptography.fernet import Fernet
import io
from datetime import datetime
import time
import warnings

# Suppress Vertica warning about transaction commit
warnings.filterwarnings('ignore', category=UserWarning, module='vertica_python')

def calculate_elapsed_time(start_time_str, end_time_str, status):
    """Calculate elapsed time between start and end time"""
    try:
        if not start_time_str:
            return None
            
        start_time = pd.to_datetime(start_time_str)
        
        if status.upper() in ['RUNNING', 'YET_TO_START']:
            end_time = datetime.now()
        elif not end_time_str:
            return None
        else:
            end_time = pd.to_datetime(end_time_str)
        
        time_diff = end_time - start_time
        hours = int(time_diff.total_seconds() // 3600)
        minutes = int((time_diff.total_seconds() % 3600) // 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"
    except Exception as e:
        print(f"Error calculating elapsed time: {e}")
        return None

def get_decrypted_credentials():
    """Decrypt and return credentials from .env file using secret.key"""
    try:
        # Read the secret key
        with open('secret.key', 'rb') as key_file:
            key = key_file.read()
        
        fernet = Fernet(key)
        
        # Read and decrypt environment variables
        creds = {}
        with open('.env', 'r') as env_file:
            for line in env_file:
                if '=' in line:
                    key, encrypted_value = line.strip().split('=', 1)
                    try:
                        decrypted_value = fernet.decrypt(encrypted_value.encode()).decode()
                        creds[key] = decrypted_value
                    except Exception as e:
                        print(f"Error decrypting {key}: {e}")
        
        return creds
    except FileNotFoundError as e:
        print(f"Error: Required file not found - {e}")
        raise
    except Exception as e:
        print(f"Error decrypting credentials: {e}")
        raise

def load_data_to_vertica():
    start_time = time.time()
    
    # Get decrypted credentials
    creds = get_decrypted_credentials()
    
    # Read the test data
    df = pd.read_csv('test_file.csv')
    
    # Convert column names to uppercase
    df.columns = df.columns.str.upper()

    def truncate_to_seconds(timestamp):
        if pd.isna(timestamp):
            return None
        # Convert to datetime and truncate to seconds
        dt = pd.to_datetime(timestamp)
        return dt.replace(microsecond=0)
    
    # Convert timestamp columns to strings without milliseconds
    timestamp_columns = ['DAG_START_TIME', 'DAG_END_TIME']
    for col in timestamp_columns:
        df[col] = (df[col]).apply(truncate_to_seconds)
    
    # Add MODIFIED_TS column with current timestamp as string
    current_time = datetime.now().replace(microsecond=0)
    df['MODIFIED_TS'] = current_time
    
    # Set DAG_END_TIME to NULL for running or yet to start status
    df.loc[df['STATUS'].str.upper().isin(['RUNNING', 'YET_TO_START']), 'DAG_END_TIME'] = None
    
    # Calculate ELAPSED_TIME
    df['ELAPSED_TIME'] = df.apply(
        lambda row: calculate_elapsed_time(
            row['DAG_START_TIME'],
            row['DAG_END_TIME'] if pd.notna(row['DAG_END_TIME']) else None,
            row['STATUS']
        ),
        axis=1
    )
    
    try:
        port = int(creds.get('PORT', '5433'))
    except (ValueError, TypeError):
        print("Warning: Invalid port number found in credentials, using default port 5433")
        port = 5433

    conn_info = {
        'host': creds.get('HOST', 'localhost'),
        'port': port,
        'user': creds.get('USER', ''),
        'password': creds.get('PASSWORD', ''),
        'database': creds.get('DATABASE', ''),
        'tlsmode': 'disable'
    }

    try:
        with vertica_python.connect(**conn_info) as conn:
            cursor = conn.cursor()
            
            # Convert DataFrame to CSV string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_data = csv_buffer.getvalue()
            
            # Use COPY command to load data
            print("Loading data using COPY command...")
            copy_sql = """
            COPY DAG_DATA (
                SUBJECT_AREA, DAG_NAME, STATUS, MODIFIED_TS,
                DAG_START_TIME, DAG_END_TIME, ELAPSED_TIME
            )
            FROM STDIN
            DELIMITER ','
            NULL AS ''
            REJECTED DATA AS TABLE DAG_DATA_REJECTS
            """
            
            cursor.copy(copy_sql, csv_data)
            conn.commit()
            
            # Print load statistics
            cursor.execute("SELECT COUNT(*) FROM DAG_DATA")
            row_count = cursor.fetchone()[0]
            
            # Print status distribution
            cursor.execute("""
                SELECT STATUS, COUNT(*) 
                FROM DAG_DATA 
                GROUP BY STATUS
            """)
            status_counts = cursor.fetchall()
            
            # Calculate total process elapsed time
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
            process_elapsed = calculate_elapsed_time(start_time_str, end_time, 'COMPLETED')
            
            print(f"\nData load completed successfully!")
            print(f"Total rows loaded: {row_count}")
            print("\nStatus distribution:")
            for status, count in status_counts:
                print(f"{status}: {count} rows")
            print(f"\nTotal elapsed time: {process_elapsed}")

    except Exception as e:
        # For error handling, also provide the status
        error_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
        error_elapsed = calculate_elapsed_time(start_time_str, error_time, 'COMPLETED')
        print(f"\nError occurred during data load!")
        print(f"Elapsed time before error: {error_elapsed}")
        print(f"Error details: {str(e)}")
        raise

def main():
    try:
        load_data_to_vertica()
    except Exception as e:
        print(f"Failed to complete data load: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()