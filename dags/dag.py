# pyright: reportMissingImports=false
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# import snowflake.connector as sc
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtSeedOperator, DbtSnapshotOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.version import version
from datetime import datetime, timedelta
import pytz
from airflow import settings
import json

# Constants
DAG_TASK_CONCURRENCY = 8 # How many tasks can be run at once
DAG_MAX_ACTIVE_RUNS = 1 # How many instances of a DAG can run at once

# https://www.astronomer.io/guides/airflow-decorators/

# Environment variables. These are set in the Dockerfile. The `AIRFLOW_VAR` is a prefix and doesn't need to be included here.
SNOWFLAKE_PASSWORD = Variable.get('snowflake_password')
CONNECTION_ID = Variable.get('CONNECTION_ID')
SLACK_CONN_ID = Variable.get('slack_connection')

def convert_datetime(datetime_string):

    return datetime_string.astimezone(pytz.timezone('America/Denver')).strftime('%b-%d %H:%M:%S')


##### Slack Alerts #####
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_msg = f"""
        :x: Task Failed.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('execution_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)

def task_succeed_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    slack_msg = f"""
        :white_check_mark: Task Succeeded!
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('execution_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id='slack_success',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)


# Default args for Airflow DAGs
default_args={
    "owner":"airflow",
    "retries":1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 12, 1),
    "catchup": False,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_succeed_slack_alert
}

# Python functions
def function_to_execute():
    '''
        Code here to execute data tasks
    '''


def sub_dag(parent_dag_name, child_dag_name):
    _dag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=default_args,
        schedule_interval="@daily",
        max_active_runs = DAG_MAX_ACTIVE_RUNS,
        concurrency = DAG_TASK_CONCURRENCY
    )

    with _dag:
        execute_function = PythonOperator(
            task_id=f'Export_table',
            python_callable=function_to_execute,
            op_kwargs= {
                'df_key' : 'df',
                'name_key' : 'table'
            }
        )
    
        execute_function
    
    return _dag


# Main dag
with DAG(
    dag_id='dags_to_use',
    schedule_interval="0 12 * * *", # Run at 5:00am MST
    start_date=datetime(2022, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs = DAG_MAX_ACTIVE_RUNS,
    concurrency = DAG_TASK_CONCURRENCY        
    ) as f:


    # Dummy operator: usually used as a start node
    t0 = DummyOperator(
        task_id='start'
    )

    # DBT Run
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir="/usr/local/airflow/dags/dbt/",
        profiles_dir='/usr/local/airflow/dags/dbt/',
        trigger_rule="all_done", # Run even if previous tasks failed
    )

    # bash = BashOperator(
    #     task_id='bash_task',
    #     bash_command='cd /usr/local/airflow/ && . dbt-env/Scripts/activate'
    # )

    # extract_nba = AirbyteTriggerSyncOperator(
    #     task_id='extract_nba',
    #     airbyte_conn_id='airbyte',
    #     connection_id=CONNECTION_ID,
    #     asynchronous=False,
    #     timeout=3600,
    #     wait_seconds=3
    # )

    # t0 >> create_tables >> dbt_seed >> dbt_snapshot >> dbt_run
    t0 >> dbt_run