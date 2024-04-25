from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from massmutual.de.laminar.operators import EDAPToolingOperator
from airflow.operators.python import PythonOperator
import pendulum
from utilites import comm_channel_notifier, custom_parameters
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.models import DagRun
from airflow..sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow import AirflowException
from airflow.models import Dag
from airflow.models import DagBag, TaskInstance
from pytz import timezone
from airflow
from urllib.parse import quote
from airflow.models import
import json
from vertica_python import connect

# ... rest of your code ...

def connect_to_vertica():
    conn_info = {
        'host': VERTICA_DB_HOST,
        'port': int(VERTICA_DB_PORT),
        'user': EDW_ENT_CALL_BATCH_USR,
        'password': EDW_ENT_CALL_BATCH_PWD,
        'database': VERTICA_DB,
        'session_label': 'some_label',
        'unicode_error': 'strict',
        'ssl': False,
        'use_prepared_statements': False,
    }
    connection = connect(**conn_info)
    print("Connection to Vertica established")
    return connection

# ... rest of your code ...

# Define the DAG
with DAG(
    "report_patrol",
    default_args= default_args,
    schedule_interval=None, 
    catchup=False, 
    tags=['DME PROD SUPPORT'], 
    max_active_runs=1
) as dag:
    # Define the task to check DAG status
    chk_dag_status = PythonOperator(
        task_id='chk_dag_status',
        python_callable=prod_support_dag_monitoring,
        provide_context=True,
        retries=0
    )

    # Define the task to connect to Vertica
    connect_vertica = PythonOperator(
        task_id='connect_vertica',
        python_callable=connect_to_vertica,
        provide_context=True,
        retries=0
    )

    # Define the task to load data into Vertica
    load_data = EDAPToolingOperator(
    # ... rest of your code ...

    # Define the task dependencies
    chk_dag_status >> connect_vertica >> load_data
