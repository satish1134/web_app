from datetime import datetime
import pandas as pd
import vertica_python
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Get the Airflow Environment variable
ENV = "{{ var.json.environment.env }}"
VERTICA_DB_HOST = "{{ var.json.vertica_conn.VERTICA_HOST }}"
VERTICA_DB_PORT = "{{ var.json.vertica_conn.VERTICA_PORT }}"
EDW_ENT_CALL_BATCH_USR = "{{ var.json.edw_ent_call_batch_usr }}"
EDW_ENT_CALL_BATCH_PWD = "{{ var.json.edw_ent_call_batch_pwd }}"
VERTICA_DB = "{{ var.json.vertica_conn.VERTICA_DB }}"

def prod_support_dag_monitoring(**context):
    df_prod_dags = pd.read_csv('/usr/local/airflow/dags/utilities/list_of_dags_to_patrol.csv', 
                               keep_default_na=False)
    subject_area_list = list(df_prod_dags["SUBJECT_AREA"].dropna().unique())
    for subject_area in subject_area_list:
        print("*" * 75)
        print("Fetching the dags status corresponding to subject_area: {0}".format(subject_area))
        df_subject_area = df_prod_dags[df_prod_dags['SUBJECT_AREA'] == subject_area]
        df_dag_status, df_task_status = get_dag_status(df_subject_area)
        if len(df_dag_status) > 0:
            send_notifications(environment, subject_area, df_dag_status, df_task_status)
            try:
                type(df_dag_status_all)
                df_dag_status_all = df_dag_status_all.append(df_task_status)
            except:
                df_dag_status_all = df_dag_status
            try:
                type(df_task_status_all)
                df_dag_status_all = df_dag_status_all.append(df_task_status)
            except:
                df_task_status_all = df_task_status
        else:
            print('no recent dag run found for {0}, not sending any mail'.format(subject_area))
    return 0

def get_dag_status(df_check_status):
    est = timezone('EST')
    df_dag_status = pd.DataFrame()
    df_failed_dag_task_details = pd.DataFrame(
        columns=['SUBJECT_AREA', 'DAG_NAME', 'TASK_ID', 'STATUS', 'TASK_URL', 'CRITICAL_DAG'], dtype=object)
    for index, row in df_check_status.iterrows():
        if len(DagRun.find(dag_id=row['DAG_NAME'], external_trigger=False)) > 0:
            dag_runs = DagRun.find(dag_id=row['DAG_NAME'], external_trigger=False)[-1]
        elif len(DagRun.find(dag_id=row['DAG_NAME'], external_trigger=True)) > 0:
            dag_runs = DagRun.find(dag_id=row['DAG_NAME'], external_trigger=True)[-1]
        else:
            print("{0}: no status found".format(row['DAG_NAME']))
            continue
        print('DAG_NAME:{0}, STATUS:{1}, START_TIME:{2}'.format(row['DAG_NAME'], dag_runs.state, dag_runs.start_date))
        if dag_runs.start_date is None:
            dag_last_run_in_days = 99
        else:
            dag_last_run_in_days = (datetime.strptime(datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                                                      "%Y-%m-%d %H:%M:%S") - datetime.strptime(
                dag_runs.start_date.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")).days
        if dag_last_run_in_days > 2:
            print("filter: dag:{0} was last run {1}".format(dag_runs.dag_id, dag_last_run_in_days))
        else:
            print("dont filter: dag:{0} was last run {1}".format(dag_runs.dag_id, dag_last_run_in_days))
        if (len(dag_runs.state) > 0) & (dag_last_run_in_days <= 2):
            df_dag_status.loc[index, 'DAG_NAME'] = row['DAG_NAME']
            df_dag_status.loc[index, 'OVERRIDE_COMM_CHANNEL'] = row['OVERRIDE_COMM_CHANNEL']
            df_dag_status.loc[index, 'SCHEDULED_RUN_TIME_IN_EST'] = row['SCHEDULED_RUN_TIME_IN_EST']
            df_dag_status.loc[index, 'SLA_EST'] = row['SLA_EST']
            df_dag_status.loc[index, 'CRITICAL_DAG'] = row['CRITICAL_DAG']
            df_dag_status.loc[index, 'STATUS'] = dag_runs.state
            df_dag_status.loc[index, "DAG_START_TIME"] = datetime.strptime(
                dag_runs.start_date.strftime("%Y-%m-%d %H:%M:%S %Z%z"), "%Y-%m-%d %H:%M:%S %Z%z").astimezone(
                est).strftime('%Y-%m-%d %H:%M:%S %Z')

            if dag_runs.end_date is not None:
                df_dag_status.loc[index, "DAG_END_TIME"] = datetime.strptime(
                    dag_runs.end_date.strftime("%Y-%m-%d %H:%M:%S %Z%z"), "%Y-%m-%d %H:%M:%S %Z%z").astimezone(
                    est).strftime('%Y-%m-%d %H:%M:%S %Z')

                elapsed_time = (datetime.strptime(dag_runs.end_date.strftime("%Y-%m-%d %H:%M:%S"),
                                                  "%Y-%m-%d %H:%M:%S") - datetime.strptime(
                    dag_runs.start_date.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"))

                min, sec = divmod(elapsed_time.total_seconds(), 60)
                hour, min = divmod(min, 60)
                df_dag_status.loc[index, "ELAPSED_TIME"] = "{0}h:{1}m:{2}sec".format(int(hour), int(min), int(sec))
            else:
                df_dag_status.loc[index, "DAG_END_TIME"] = "9999-12-31 00:00:00"
                df_dag_status.loc[index, "ELAPSED_TIME"] = " "
            modified_ts = datetime.strptime(datetime.now(timezone('utc')).strftime("%Y-%m-%d %H:%M:%S %Z%z"),
                                            "%Y-%m-%d %H:%M:%S %Z%z").astimezone(est).strftime('%Y-%m-%d %H:%M:%S')

            df_dag_status.loc[index, "MODIFIED_TS"] = modified_ts

            if dag_runs.state.upper() != 'SUCCESS':
                dag_bag = DagBag(
                    dag_folder="/usr/local/airflow/dags/{0}/{1}.py".format(row['SUBJECT_AREA'], dag_runs.dag_id),
                    include_example=False)

                for dag in dag_bag.dag.values():
                    for task in dag.tasks:
                        iso = quote(dag_runs.execution_date.isoformat())
                        base_url = conf.get("webserver", "BASE_URL")
                        TI = TaskInstance(task=task, execution_date=dag_runs.execution_date, run_id=None, state=None)
                        status = TI.current_state()
                        key = "{0}.{1}".format(dag.dag_id, task.task_id)
                        try:
                            df_failed_dag_task_details.loc[key, 'SUBJECT_AREA'] = dag.dag_id
                            df_failed_dag_task_details.loc[key, 'TASK_ID'] = task.task_id
                            df_failed_dag_task_details.loc[key, 'STATUS'] = status
                            log_url = "{base_url}/log?execution_date={iso}&task_id={task_id}&dag_id={dag_id}&map_index={map_index}".format(
                                base_url=base_url, iso=iso, task_id=task.task_id, dag_id=dag.dag_id, map_index='-1')
                            df_failed_dag_task_details.loc[key, 'TASK_URL'] = log_url
                            df_failed_dag_task_details.loc[key, 'CRITICAL_DAG'] = row['CRITICAL_DAG']
                            df_failed_dag_task_details.loc[key, "MODIFIED_TS"] = modified_ts
                        except Exception as e:
                            print('exception while getting the task details.')
                            print(e)
                            raise AirflowException

        if (len(df_dag_status) == 0) or (len(df_failed_dag_task_details) == 0):
            df_failed_dag_task_details = pd.DataFrame(
                columns=['SUBJECT AREA', 'DAG_NAME', 'TASK_ID', 'STATUS', 'TASK_URL', 'CRITICAL_DAG', 'MODIFIED_TS'],
                dtype=object)
        elif (len(df_dag_status) > 0) & (len(df_failed_dag_task_details) > 0):
            df_failed_dag_task_details = df_failed_dag_task_details[
                (df_failed_dag_task_details['STATUS'].notnull()) & (
                        df_failed_dag_task_details['STATUS'] != "upstream_failed")
                & (df_failed_dag_task_details['STATUS'] != 'success')]
            df_failed_dag_task_details.reset_index(drop=True, inplace=True)
            df_failed_dag_task_details.sort_values(by=['STATUS', 'DAG_NAME', 'TASK_ID'], inplace=True)
            df_dag_status.sort_values(by=['STATUS', 'SUBJECT_AREA', 'DAG_NAME'], inplace=True)
        return df_dag_status, df_failed_dag_task_details

# Define the DAG
with DAG(
    "report_patrol",
    default_args={
        'owner': 'DME_PROD_SUPP_TEAM',
        'start_date': datetime(2023, 4, 27, 13, 0),
        'depends_on_past': False,
        'email_on_failure': True,
    },
    schedule_interval='00 8 * * *', 
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

    # Define the task to load data into Vertica
    load_data = EDAPToolingOperator(
    service_account_name=SERVICE_ACCOUNT_NAME,
    db_xcom_push=False,
    task_id='load_data_to_vertica',
    is_delete_operator_pod=True,
    env_vars={
        "VERTICA_DB_HOST": VERTICA_DB_HOST,
        "VERTICA_DB_PORT": VERTICA_DB_PORT,
        "DB_USER": EDW_ENT_CALL_BATCH_USR,
        "VERTICA_PASSWORD": EDW_ENT_CALL_BATCH_PWD,
        "VERTICA_DB": VERTICA_DB,
        "VERTICA_DB_PORT": VERTICA_DB_PORT
    },
    labels={"app": "kubrpod"},
    in_cluster=False,
    cmds=[
       "bash",
       "-euxo",
       "pipefall",
       "-c",
       "set -e",
       "source virt/bin/activate ; python -c '"
       "import pandas as pd\n"
       "import vertica_python\n"
       "import os\n"
       "ENV = \"{0}\"\n".format(ENV),
       "VERTICA_DB_HOST = \"{0}\"\n".format(VERTICA_DB_HOST),
       "VERTICA_DB_PORT = \"{0}\"\n".format(VERTICA_DB_PORT),
       "DB_USER = \"{0}\"\n".format(EDW_ENT_CALL_BATCH_USR),
       "VERTICA_PASSWORD = \"{0}\"\n".format(EDW_ENT_CALL_BATCH_PWD),
       "VERTICA_DB = \"{0}\"\n".format(VERTICA_DB),
       "connection_info = {\n"
       "    'host': VERTICA_DB_HOST,\n"
       "    'port': VERTICA_DB_PORT,\n"
       "    'user': DB_USER,\n"
       "    'password': VERTICA_PASSWORD,\n"
       "    'database': VERTICA_DB\n"
       "}\n"
       "try:\n"
       "    connection = vertica_python.connect(**connection_info)\n"
       "    cursor = connection.cursor()\n"
       "    create_table_query = '''\n"
       "    CREATE TABLE IF NOT EXISTS report_table (\n"
       "        SUBJECT_AREA VARCHAR(255),\n"
       "        DAG_NAME VARCHAR(255),\n"
       "        STATUS VARCHAR(50),\n"
       "        DAG_START_TIME TIMESTAMP WITH TIME ZONE,\n"
       "        DAG_END_TIME TIMESTAMP WITH TIME ZONE,\n"
       "        ELAPSED_TIME INTERVAL HOUR TO SECOND,\n"
       "        LOAD_TIMESTAMP TIMESTAMP\n"
       "    )'''\n"
       "    cursor.execute(create_table_query)\n"
       "    connection.commit()\n"
       "    data = construct_data_for_vertica(df_dag_status)\n"
       "    cursor.executemany('''"
       "    INSERT INTO report_table (SUBJECT_AREA, DAG_NAME, STATUS, DAG_START_TIME, DAG_END_TIME, ELAPSED_TIME, LOAD_TIMESTAMP)"
       "    VALUES (?, ?, ?, ?, ?, ?, ?)"
       "    ''', data)\n"
       "    connection.commit()\n"
       "    cursor.close()\n"
       "    connection.close()\n"
       "except Exception as e:\n"
       "    print('Failed to connect to Vertica:', e)\n"
       "    raise AirflowException('Failed to connect to Vertica')\n"
       "'", 
    ],
    retries=0
)


# Define the task dependencies
chk_dag_status >> load_data
