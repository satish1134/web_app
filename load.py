import vertica_python
import boto3
import json
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone as airflow_timezone

# Define the AWS region where your secrets are stored
aws_region = 'your_aws_region'

# Create a Secrets Manager client
secrets_manager_client = boto3.client('secretsmanager', region_name=aws_region)

# Function to retrieve Vertica credentials from AWS Secrets Manager
def get_vertica_credentials():
    secret_name = 'your_vertica_secret_name'
    try:
        # Retrieve the secret value from AWS Secrets Manager
        response = secrets_manager_client.get_secret_value(SecretId=secret_name)
        secret_string = response['SecretString']
        secret_dict = json.loads(secret_string)
        return {
            'host': secret_dict['host'],
            'port': secret_dict['port'],
            'user': secret_dict['username'],
            'password': secret_dict['password'],
            'database': secret_dict['database']
        }
    except Exception as e:
        print("Error retrieving Vertica credentials from AWS Secrets Manager:", e)
        raise

# Retrieve Vertica credentials
vertica_connection = get_vertica_credentials()

# Establish Vertica connection
connection = vertica_python.connect(**vertica_connection)

# Define SQL query to create the Vertica table
create_table_query = """
CREATE TABLE IF NOT EXISTS report_table (
    SUBJECT_AREA VARCHAR(255),
    DAG_NAME VARCHAR(255),
    STATUS VARCHAR(50),
    DAG_START_TIME TIMESTAMP WITH TIME ZONE,
    DAG_END_TIME TIMESTAMP WITH TIME ZONE,
    ELAPSED_TIME INTERVAL HOUR TO SECOND,
    LOAD_TIMESTAMP TIMESTAMP
)
"""

# Execute the create table query
with connection.cursor() as cur:
    cur.execute(create_table_query)

# Define DAG arguments
args = {
    'owner': 'DME_PROD_SUPP_TEAM',
    'start_date': datetime(2023, 4, 27, 13, 0),
    'depends_on_past': False,
    'email_on_failure': True,
}

# Define DAG
dag = DAG(
    'report_patrol',
    default_args=args,
    schedule_interval='00 8 * * *',
    catchup=False,
    tags=['DME PROD SUPPORT'],
    max_active_runs=1
)

# PythonOperator to monitor DAG status and load data into Vertica
chk_dag_status = PythonOperator(
    task_id='chk_report_patrol',
    python_callable=prod_support_dag_monitoring,
    provide_context=True,
    dag=dag
)

# Function to construct data for Vertica insertion
def construct_data_for_vertica(df_dag_status):
    data = []
    for index, row in df_dag_status.iterrows():
        data.append((
            row['SUBJECT_AREA'],
            row['DAG_NAME'],
            row['STATUS'],
            row['DAG_START_TIME'],
            row['DAG_END_TIME'],
            row['ELAPSED_TIME'],
            airflow_timezone.utcnow()  # Record the timestamp when the data is loaded
        ))
    return data

# Function to load data into Vertica
def load_data_to_vertica(data):
    with connection.cursor() as cur:
        # Truncate the table first
        cur.execute("TRUNCATE TABLE report_table")
        # Load data into the table
        cur.executemany(
            "INSERT INTO report_table (SUBJECT_AREA, DAG_NAME, STATUS, DAG_START_TIME, DAG_END_TIME, ELAPSED_TIME, LOAD_TIMESTAMP) VALUES (?, ?, ?, ?, ?, ?, ?)",
            data
        )
    connection.commit()

# Function to monitor DAG status and load data into Vertica
def prod_support_dag_monitoring(**context):
    # Existing logic to fetch data
    df_prod_dags = pd.read_csv('/usr/local/airflow/dags/utilities/list_of_dags_to_patrol.csv', keep_default_na=False)
    subject_area_list = list(df_prod_dags["SUBJECT_AREA"].dropna().unique())
    for subject_area in subject_area_list:
        df_subject_area = df_prod_dags[df_prod_dags['SUBJECT_AREA'] == subject_area]
        df_dag_status, _ = get_dag_status(df_subject_area)
        if len(df_dag_status) > 0:
            # Construct data for Vertica insertion
            data_for_vertica = construct_data_for_vertica(df_dag_status)
            # Load data into Vertica
            load_data_to_vertica(data_for_vertica)
    return 0

# Add the PythonOperator to the DAG
chk_dag_status
