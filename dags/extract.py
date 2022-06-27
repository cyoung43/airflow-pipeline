# Retrieve logs and check if they are older than 30 days

import os
import re
import shutil
import pandas as pd
from datetime import datetime
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

#example = "/usr/local/airflow/logs/dag_id=example_dag_basic/run_id=manual__2022-06-10T21:47:56.0+00:00/task_id=extract.extract_timecards/attempt=1.log"
"""
Things to know:
    - base_path is the path to the logs folder. Path can be found in any log file. Want to copy everything up to, but excluding "dag_id="
        e.g. /usr/local/airflow/logs/dag_id=example --> base_path = "/usr/local/airflow/logs"
    - base_path will be left as None by default. Unless changed, runtime will be slightly longer
"""

############ Variables and Constants #############

#base_path = None       # Base path can be found in top of log files. Should like similar to: /usr/local/airflow/logs/
base_path = "/usr/local/airflow/logs" 
current_day = str(datetime.now().date())
DATE_REGEX = r'\d{4}-\d{2}-\d{2}' # matches YYYY-MM-DD
TIME_REGEX = r'\d{2}:\d{2}:\d{2}' # change , to : in final version # Matches HH:MM:SS
AGE_DAYS_THRESHOLD = 0



############ Functions for extracting data from logs #############

def build_dataframe(all_paths):
    df = pd.DataFrame(columns=['path', 'dag_id', 'date_of_run', 'task_id', 'attempt_number', 'time_of_run', 'file_content', 'success_flag'])

    print("\n")
    print("Creating dataframe...")
    # Check if greater than 30 days
    for path in all_paths:
        if "dag_id=" in path:
            date_to_compare = date_extract(path)

            if date_diff(current_day, date_to_compare) >= AGE_DAYS_THRESHOLD:

                # extract information from file path and add to dataframe
                path_data = extract_path_data(path)
                df.loc[len(df.index)] = path_data

    return df

def date_extract(path, date_regex=DATE_REGEX):
    print(TIME_REGEX)
    print(path)
    extracted_date = re.findall(date_regex, path)
    print(extracted_date)
    extracted_date = extracted_date[0]
    return extracted_date

def date_diff(date1, date2):
    #converts string to datetime objects
    date1 = datetime.strptime(date1, '%Y-%m-%d')
    date2 = datetime.strptime(date2, '%Y-%m-%d')
    
    delta = date1 - date2

    return delta.days

def delete_old_folders(df):
    for path in df['path']:

        dag_id = re.findall(r'dag_id=([A-Za-z0-9_-]+)', path)  # all letters, digits, underscore, and hyphen
        run_id = re.findall(r'run_id=([A-Za-z0-9_.:,+-]+)', path)  # all letters, digits, underscore, period, colon, plus, hyphen

        # grab path before dag_id
        path_before_dag_id = path.split("\dag_id=")[0]
        
        # Delete run_id folder
        if base_path is None:
            path_to_delete = path_before_dag_id + "/dag_id=" + dag_id[0] + "/run_id=" + run_id[0]
        elif base_path is not None:
            path_to_delete = base_path + "/dag_id=" + dag_id[0] + "/run_id=" + run_id[0]


        if os.path.exists(path_to_delete):
            # print(f'\t Deleting: {path_to_delete}')
            shutil.rmtree(path_to_delete) # delete folder with all contents

def extract_path_data(path):
    path_data = {}
    path_data['path'] = path

    my_dag_id = re.findall(r'dag_id=([A-Za-z0-9_-]+)', path)  # all letters, digits, underscore, and hyphen  
    my_dag_id = my_dag_id[0]

    date_of_run = date_extract(path)
    time_of_run = time_extract(path)

    task_id = re.findall(r'task_id=([A-Za-z0-9_.]+)', path) # all letters, digits, underscore, and period
    task_id = task_id[0]

    attempt_number = re.findall(r'attempt=(\w+)', path) # w stands for "word character", usually [A-Za-z0-9_]. Notice the inclusion of the underscore and digits
    attempt_number = int(attempt_number[0])

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

    return path_data

def find_files(path='/'):
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

def time_extract(path, time_regex=TIME_REGEX):
    extracted_time = re.findall(time_regex, path)
    extracted_time = extracted_time[0]
    return extracted_time

def upload_dataframe_to_snowflake(df):
    print("\n")
    print("Opening connection to snowflake...")
    conn = snowflake.connector.connect(
        user='nperez',
        password='AUGm%1l4Cf^C24w1gOQvRv%B%lRT^q3i2',
        account='ex89663.west-us-2.azure',
        warehouse='AIRFLOW_TESTING',
        database='DB_AIRFLOW_LOGS',
        schema='SCHEMA_AIRFLOW_TEST',
    )

    success, nchunks, nrows, _ = write_pandas(conn, df, 'logs_data', quote_identifiers=False)
    conn.close()
    print("Snowflake connection closed.")
    return success, nrows



######## Code Execution #########

def execute():
    print("Starting...")
    if base_path:
        print("Using supplied base path")
        all_paths = find_files(base_path)
    else:
        print("Using root as path. This may take a while...")
        all_paths = find_files()

    df = build_dataframe(all_paths)
    total_logs = len(df.index)

    print(df)

    # upload dataframe to snowflake
    success = False
    if total_logs > 0:
        print(f'Found {total_logs} logs older than {AGE_DAYS_THRESHOLD} days.')

        success, nrows = upload_dataframe_to_snowflake(df)
        
        print(f'Success: {success} \t Records added: {nrows}')
    else:
        print(f"No data uploaded. No logs older than {AGE_DAYS_THRESHOLD} days were found.")

    # delete old folders
    print("\n")
    if success == True and nrows == total_logs:
        print(f"Deleting old logs from Airflow...")
        delete_old_folders(df)

    print("Done.")
