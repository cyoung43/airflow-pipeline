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


with DAG(
    dag_id="Sharetown-prod",
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

    t0 >> dbt_seed >> dbt_snapshot >> dbt_run

