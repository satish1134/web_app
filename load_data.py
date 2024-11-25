import pandas as pd
import vertica_python
from cryptography.fernet import Fernet
import io

def get_decrypted_credentials():
    # Previous implementation remains the same
    with open('secret.key', 'rb') as key_file:
        key = key_file.read()
    
    f = Fernet(key)
    
    with open('.env', 'r') as env_file:
        env_vars = {}
        for line in env_file:
            line = line.strip()
            if line:
                try:
                    key, encrypted_value = line.split('=', 1)
                    key = key.strip()
                    encrypted_value = encrypted_value.strip()
                    if key and encrypted_value:
                        try:
                            decrypted_value = f.decrypt(encrypted_value.encode()).decode()
                            env_vars[key] = decrypted_value
                        except Exception as e:
                            print(f"Error decrypting value for {key}: {str(e)}")
                except ValueError as e:
                    print(f"Skipping invalid line: {line}")
                    continue
    
    return env_vars

def load_data_to_vertica():
    # Get decrypted credentials
    creds = get_decrypted_credentials()
    
    # Read the test data
    df = pd.read_csv('test_file.csv')  # Changed file name here
    
    # Create connection configuration
    conn_info = {
        'host': creds['HOST'],
        'port': int(creds['PORT']),
        'user': creds['USER'],
        'password': creds['PASSWORD'],
        'database': creds['DATABASE'],
        'tlsmode': 'disable'
    }

    # Create table query using column names from the CSV
    columns = df.columns
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS dag_data (  
        {', '.join([f'"{col}" VARCHAR' for col in columns])}
    )
    """
    
    try:
        # Connect to Vertica
        with vertica_python.connect(**conn_info) as conn:
            cursor = conn.cursor()
            
            # Create table
            print("Creating table if it doesn't exist...")
            cursor.execute(create_table_sql)
            
            # Convert DataFrame to CSV string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_data = csv_buffer.getvalue()
            
            # Use COPY command to load data
            print("Loading data using COPY command...")
            copy_sql = f"""
            COPY dag_data ({','.join([f'"{col}"' for col in columns])})
            FROM STDIN
            DELIMITER ','
            NULL AS ''
            REJECTED DATA AS TABLE dag_data_rejects
            """
            
            cursor.copy(copy_sql, csv_data)
            conn.commit()
            
            # Check for rejected rows
            cursor.execute("SELECT COUNT(*) FROM dag_data_rejects")
            reject_count = cursor.fetchone()[0]
            
            if reject_count > 0:
                print(f"Warning: {reject_count} rows were rejected.")
                print("Checking rejected rows:")
                cursor.execute("SELECT * FROM dag_data_rejects LIMIT 5")
                rejects = cursor.fetchall()
                for reject in rejects:
                    print(reject)
            
            # Get number of rows loaded
            cursor.execute("SELECT COUNT(*) FROM dag_data")
            loaded_rows = cursor.fetchone()[0]
            print(f"Successfully loaded {loaded_rows} rows into Vertica")
            
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    try:
        import vertica_python
    except ImportError:
        print("Installing required package: vertica-python")
        os.system("pip install vertica-python")
    
    load_data_to_vertica()
