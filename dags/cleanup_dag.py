from distutils import log
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import date, datetime, timedelta
from datetime import datetime
# from airflow.models import Variable
from extract import Log_Cleanup
from rsf_snowflake import Snowflake

days_threshold = 0
base_path = "/"

########### CONSTANTS ###########
# RETRIES = int(Variable.get('default_retries'))
# DBT_LOCATION = Variable.get('dbt_location')
# DAG_MAX_ACTIVE_RUNS = int(Variable.get('default_dag_max_active_runs'))
# DAG_TASK_CONCURRENCY = int(Variable.get('default_dag_task_concurrency'))
# AZURE_BLOB_CONN_STRING_SECRET = Variable.get('azure_blob_conn_string_secret')
# AZURE_BLOB_CONTAINER = Variable.get('azure_blob_container')
DATABASE_NAME = "DB_AIRFLOW_LOGS"
WAREHOUSE_NAME = "AIRFLOW_TESTING"
SNOWFLAKE_USERNAME = "nperez"
SNOWFLAKE_PASSWORD = "AUGm%1l4Cf^C24w1gOQvRv%B%lRT^q3i2"
SNOWFLAKE_ACCOUNT = "ex89663.west-us-2.azure"
SNOWFLAKE_SCHEMA = "SCHEMA_AIRFLOW_TEST"
SNOWFLAKE_STAGE_NAME = "STAGE_AIRFLOW_TEST"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    "start_date": datetime(2021, 12, 1),
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'airflow_cleanup',
    start_date=datetime(2022, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['maintenance', 'standalone']
)

def taskflow(base_path, days_threshold):
    '''
    ### Airflow Maintenance DAG
    '''

    snowflake = Snowflake(
        SNOWFLAKE_ACCOUNT, 
        DATABASE_NAME, 
        WAREHOUSE_NAME, 
        SNOWFLAKE_USERNAME, 
        SNOWFLAKE_PASSWORD, 
        SNOWFLAKE_STAGE_NAME, 
        SNOWFLAKE_SCHEMA
    )

    logs = Log_Cleanup(snowflake, base_path, days_threshold)

    # 'DATA_LAKE.PUBLIC.AZURE_DATA_LAKE' - Azure bucket name
    # 'DATA_LAKE.PUBLIC.MYSQL_CSV' - Formatting csv (file_format)

    log_cleanup_tasks = logs.create_task_group('logs', 'dump', snowflake, default_args)

    bash = BashOperator(
        task_id='bash_task',
        bash_command='cd /usr/local/airflow/logs/dag_id=airflow_cleanup && ls -a',
        
    )

    bash2 = BashOperator(
        task_id='bash2_task',
        bash_command='cd /usr/local/airflow/logs/dag_id=airflow_cleanup && ls -a',
        
    )

    t0 = DummyOperator(task_id='t0')

    t0 >> bash >> [task for task in log_cleanup_tasks] >> bash2


dag = taskflow(base_path, days_threshold)



