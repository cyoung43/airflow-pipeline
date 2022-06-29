
# pyright: reportMissingImports=false
# pyright: reportMissingModuleSource=false

import os
import re
import shutil
import pandas as pd
from datetime import datetime
import snowflake.connector as sc
from snowflake.connector.pandas_tools import write_pandas
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

###########
from rsf_snowflake import Snowflake
# from rsf_azure_blob import AzureBlob

"""
Things to know:
    - base_path is the path to the logs folder. Path can be found in any log file. Want to copy everything up to, but excluding "/dag_id="
        e.g. /usr/local/airflow/logs/dag_id=example --> base_path = "/usr/local/airflow/logs"
    - base_path will be left as None by default. Unless changed, runtime will be slightly longer
"""
############ Variables and Constants #############

#base_path = None   
# base_path = "/usr/local/airflow/logs" 

# TO DO: move to init as default parameters and class variables
current_day = str(datetime.now().date())
DATE_REGEX = r'\d{4}-\d{2}-\d{2}' # matches YYYY-MM-DD
TIME_REGEX = r'\d{2},\d{2},\d{2}' # change , to : in final version # Matches HH:MM:SS
# AGE_DAYS_THRESHOLD = 30

class Log_Cleanup():
    def __init__(self, snowflake_connection, base_path="/", age_days_threshold=30):
        self.snowflake = snowflake_connection
        self.base_path = base_path
        self.age_days_threshold = age_days_threshold
        

    ############ Functions for extracting data from logs #############

    def build_dataframe(self, all_paths):
        df = pd.DataFrame(columns=['dag_id', 'date_of_run', 'time_of_run', 'task_id', 'date_inserted', 'file_content', 'attempt_number', 'path', 'success_flag'])
        #df = pd.DataFrame(columns=['path', 'dag_id', 'date_of_run', 'task_id', 'attempt_number', 'time_of_run', 'file_content', 'success_flag'])

        print("\n")
        print("Creating dataframe...")
        # Check if greater than 30 days
        for path in all_paths:
            if "dag_id=" in path:
                date_to_compare = self.date_extract(path)

                if self.date_diff(current_day, date_to_compare) >= self.age_days_threshold:

                    # extract information from file path and add to dataframe
                    path_data = self.extract_path_data(path)
                    df.loc[len(df.index)] = path_data

        return df
    
    def create_task_group(self, endpoints, dump_object, load_object, default_args, time_extract = False, time_api_key = None):
        """Creates a task group (airflow UI grouping method) for the list of tables

            Parameters:
                endpoints (list: dict): list of dictionary objects containing endpoint name and columns to select
                    - endpoint = {
                        'name': (str) name of endpoint,
                        'columns_filter': (list/optional) list of columns to select
                        'params': (str/optional) table to query snowflake to get the necessary fields to iterate on
                        'query_name': (str/optional) query name to help build query string for the endpoint
                        'url_pt_2': (str/optional) url part 2. This is the second half of the api endpoint, if there is one
                        }
                dump_object (EbDumpInterface): object to dump data to Azure blob
                load_object (EbLoadInterface): object to load data (Snowflake)
                default_args (dict): default arguments for task group
                time_extract (bool/optional): whether or not this is a time & attendance extraction. Defaults to False
                time_api_key (str/optional): api key for time & attendance extraction. Defaults to None
        
            Returns:
                task_group (list): list of task group objects
        """

        task_groups = []

        for endpoint in endpoints:

            tg = TaskGroup(
                group_id=endpoint['table_name'] if 'table_name' in endpoint else endpoint['endpoint'],
                default_args=default_args,
            )

            with tg:
                execute_extract = PythonOperator(
                    task_id=f'extract_{endpoint["table_name"] if "table_name" in endpoint else endpoint["endpoint"]}',
                    python_callable=self.time_extract if time_extract else self.extract,
                    op_kwargs={
                        'endpoint': endpoint['endpoint'],
                        'dump_obj': dump_object,
                        'load_obj': load_object,
                        'columns_filter': endpoint['columns_filter'] if 'columns_filter' in endpoint.keys() else None,
                        'api_key': time_api_key,
                        'params': endpoint['params'] if 'params' in endpoint.keys() else None,
                        'query_name': endpoint['query_name'] if 'query_name' in endpoint.keys() else None,
                        'url_pt_2': endpoint['url_pt_2'] if 'url_pt_2' in endpoint.keys() else '',
                    }
                )

                execute_create_table = PythonOperator(
                    task_id=f'create_{endpoint["table_name"] if "table_name" in endpoint else endpoint["endpoint"]}',
                    python_callable=self.create_table,
                    op_kwargs={
                        'endpoint': endpoint['table_name'] if 'table_name' in endpoint else endpoint['endpoint'],
                        'load_obj': load_object,
                        'replace': False,
                        'group_task': True,
                        'full_name': endpoint['table_name'] if 'table_name' in endpoint else endpoint['endpoint'],
                    }
                )

                execute_table_import = PythonOperator(
                    task_id=f'import_{endpoint["table_name"] if "table_name" in endpoint else endpoint["endpoint"]}',
                    python_callable=self.load,
                    op_kwargs={
                        'endpoint': endpoint['table_name'] if 'table_name' in endpoint else endpoint['endpoint'],
                        'load_obj': load_object,
                        'group_task': True,
                        'full_name': endpoint['table_name'] if 'table_name' in endpoint else endpoint['endpoint'],
                    }
                )

                execute_extract >> execute_create_table >> execute_table_import

            task_groups.append(tg)
        
        return task_groups

    def date_extract(self, path, date_regex=DATE_REGEX):
        # print(TIME_REGEX)
        # print(path)
        extracted_date = re.findall(date_regex, path)
        # print(extracted_date)
        extracted_date = extracted_date[0]
        return extracted_date

    def date_diff(self, date1, date2):
        #converts string to datetime objects
        date1 = datetime.strptime(date1, '%Y-%m-%d')
        date2 = datetime.strptime(date2, '%Y-%m-%d')
        
        delta = date1 - date2

        return delta.days

    def delete_old_folders(self, df):
        for path in df['path']:

            dag_id = re.findall(r'dag_id=([A-Za-z0-9_-]+)', path)  # all letters, digits, underscore, and hyphen
            run_id = re.findall(r'run_id=([A-Za-z0-9_.:,+-]+)', path)  # all letters, digits, underscore, period, colon, plus, hyphen

            # grab path before dag_id
            path_before_dag_id = path.split("\dag_id=")[0]
            
            # Delete run_id folder
            if self.base_path is None:
                path_to_delete = path_before_dag_id + "/dag_id=" + dag_id[0] + "/run_id=" + run_id[0]
            elif self.base_path is not None:
                path_to_delete = self.base_path + "/dag_id=" + dag_id[0] + "/run_id=" + run_id[0]


            if os.path.exists(path_to_delete):
                # print(f'\t Deleting: {path_to_delete}')
                shutil.rmtree(path_to_delete) # delete folder with all contents

    def extract_path_data(self, path):
        path_data = {}
        path_data['path'] = path

        my_dag_id = re.findall(r'dag_id=([A-Za-z0-9_-]+)', path)  # all letters, digits, underscore, and hyphen  
        my_dag_id = my_dag_id[0]

        date_of_run = self.date_extract(path)
        time_of_run = self.time_extract(path)

        task_id = re.findall(r'task_id=([A-Za-z0-9_.]+)', path) # all letters, digits, underscore, and period
        task_id = task_id[0]

        attempt_number = re.findall(r'attempt=(\w+)', path) # w stands for "word character", usually [A-Za-z0-9_]. Notice the inclusion of the underscore and digits
        attempt_number = int(attempt_number[0])

        insertion_date = str(datetime.now().date())

        with open(path, "r") as file:
            lines = file.readlines()
            
            sOutput = ""

            for line in lines:
                sOutput += line

            file_content = sOutput

            # Check file content if success or failure
            success_flag = "None"

            if re.search(r'Marking task as SUCCESS', file_content):
                success_flag = "SUCCESS"
            elif re.search(r'Marking task as FAILED', file_content):
                success_flag = "FAILED"
            elif re.search(r'Marking task as SKIPPED', file_content):
                success_flag = "SKIPPED"

        path_data['dag_id'] = my_dag_id
        path_data['date_of_run'] = date_of_run  # date_of_run comes from the run_id
        path_data['task_id'] = task_id
        path_data['attempt_number'] = attempt_number
        path_data['time_of_run'] = time_of_run
        path_data['file_content'] = file_content
        path_data['success_flag'] = success_flag
        path_data['date_inserted'] = insertion_date

        return path_data

    def find_files(self, path='/'):
        list_of_paths = []

        for dirpath, dirnames, filenames in os.walk(path):

            # path was provided
            if path != '/':
                for filename in filenames:
                    file_path = os.path.join(dirpath, filename)
                    list_of_paths.append(file_path)

            # default execution. Slower run time
            elif path == '/':
                if "/logs/dag_id" in dirpath:
                    for filename in filenames:
                        file_path = os.path.join(dirpath, filename)
                        list_of_paths.append(file_path)

                if "\logs\\dag_id" in dirpath:
                    for filename in filenames:
                        file_path = os.path.join(dirpath, filename)
                        list_of_paths.append(file_path)

        return list_of_paths

    def time_extract(self, path, time_regex=TIME_REGEX):
        extracted_time = re.findall(time_regex, path)
        extracted_time = extracted_time[0]
        return extracted_time

    def upload_dataframe_to_snowflake(self, df):
        print("\n")
        print("Opening connection to snowflake...")
        snowflake = Snowflake(
            user='nperez',
            account = 'ex89663.west-us-2.azure',
            database='DB_AIRFLOW_LOGS',
            warehouse='AIRFLOW_TESTING',
            password='AUGm%1l4Cf^C24w1gOQvRv%B%lRT^q3i2',
            stage_name='STAGE_AIRFLOW_TEST',
            schema='SCHEMA_AIRFLOW_TEST',
        )


        conn = sc.connect(
            user=snowflake.user,
            password=snowflake.password,
            account=snowflake.account,
            warehouse=snowflake.warehouse,
            database=snowflake.database,
            schema=snowflake.schema,
        )

        # Create table in database
        print("Creating table in database...")
        my_types = dict(df.dtypes)

        #TODO: make this more robust
        
        snowflake.create_target_table("Testing", columns=df.columns, data_types=my_types, schema=snowflake.schema, replace=False)

        success, nchunks, nrows, _ = write_pandas(conn, df, 'Testing', quote_identifiers=False)
        conn.close()
        print("Snowflake connection closed.")
        return success, nrows

    

    ######## Code Execution #########

    def execute(self):
        print("Starting...")
        if self.base_path:
            print("Using supplied base path")
            all_paths = self.find_files(self.base_path)
        else:
            print("Using root as path. This may take a while...")
            all_paths = self.find_files()

        df = self.build_dataframe(all_paths)
        total_logs = len(df.index)

        print(df)

        # write to csv
        #azure_blob.dump(df)

        # upload dataframe to snowflake
        success = False
        if total_logs > 0:
            
            print(f'Found {total_logs} logs older than {self.age_days_threshold} days.')

            success, nrows = self.upload_dataframe_to_snowflake(df)
            
            print(f'Success: {success} \t Records added: {nrows}')
        else:
            print(f"No data uploaded. No logs older than {self.age_days_threshold} days were found.")

        # delete old folders
        print("\n")
        if success == True and nrows == total_logs:
            print(f"Deleting old logs from Airflow...")
            self.delete_old_folders(df)

        print("Done.")


snowflake = Snowflake(
            user='nperez',
            account = 'ex89663.west-us-2.azure',
            database='DB_AIRFLOW_LOGS',
            warehouse='AIRFLOW_TESTING',
            password='AUGm%1l4Cf^C24w1gOQvRv%B%lRT^q3i2',
            stage_name='STAGE_AIRFLOW_TEST',
            schema='SCHEMA_AIRFLOW_TEST',
        )

my_object = Log_Cleanup(snowflake, "/Users/np1356/Desktop/Airflow/airflow-dbt-starter/logs")
my_object.execute()