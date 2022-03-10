# pyright: reportMissingImports=false
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import snowflake.connector as sc
from airflow.models import Variable
from airflow.operators.dbt_operator import DbtRunOperator, DbtSeedOperator, DbtSnapshotOperator
from airflow.version import version
from datetime import datetime, timedelta


# Default args for Airflow DAGs
default_args={
    "owner":"airflow",
    "retries":1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 12, 1),
    "catchup": False,
}


# Constants
DAG_TASK_CONCURRENCY = 8 # How many tasks can be run at once
DAG_MAX_ACTIVE_RUNS = 1 # How many instances of a DAG can run at once


# Environment variables. These are set in the Dockerfile. The `AIRFLOW_VAR` is a prefix and doesn't need to be included here.
SNOWFLAKE_PASSWORD = Variable.get('snowflake_password')


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
            op_kwargs= {'df_key' : 'df',
            'name_key' : 'table'}
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

    create_tables = SubDagOperator(
        task_id='create_tables',
        subdag=sub_dag('dags_to_use', 'example_sub_dag')
    )


    # DBT Seed
    dbt_seed = DbtSeedOperator(
        task_id='dbt_seed',
        dir="/usr/local/airflow/dags/dbt/",
        profiles_dir='/usr/local/airflow/dags/dbt/',
        trigger_rule="all_done", # Run even if previous tasks failed
        full_refresh=True # Ensures that the table can be recreated when different datatypes are detected.
    )

    # DBT Snapshot
    dbt_snapshot = DbtSnapshotOperator(
        task_id='dbt_snapshot',
        dir="/usr/local/airflow/dags/dbt/",
        profiles_dir='/usr/local/airflow/dags/dbt/',
        trigger_rule="all_done", # Run even if previous tasks failed
    )

    # DBT Run
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir="/usr/local/airflow/dags/dbt/",
        profiles_dir='/usr/local/airflow/dags/dbt/',
        trigger_rule="all_done", # Run even if previous tasks failed
    )

    t0 >> create_tables >> dbt_seed >> dbt_snapshot >> dbt_run
