from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import date, datetime, timedelta
from datetime import datetime
from airflow.models import Variable
from extract import execute

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
def taskflow():
    '''
    ### Airflow Maintenance DAG
    '''

    log_cleanup = PythonOperator(
        task_id='log_cleanup',
        python_callable=execute,
        op_kwargs={}
    )

    bash = BashOperator(
        task_id='bash_task',
        bash_command='cd /usr/local/airflow/logs/dag_id=airflow_cleanup && ls -a',
        
    )

    bash2 = BashOperator(
        task_id='bash2_task',
        bash_command='cd /usr/local/airflow/logs/dag_id=airflow_cleanup && ls -a',
        
    )

    t0 = DummyOperator(task_id='t0')

    t0 >> bash >> log_cleanup >> bash2

dag = taskflow()


