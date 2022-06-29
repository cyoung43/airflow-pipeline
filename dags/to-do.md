#### To do:
- [ ] Standardize the code to be class-based (template off of rsf_rippling)
- [ ] Integrate the azure blob piece
    * Stage data before uploading to snowflake (as a csv)
- [ ] Integrate snowflake functions
    * these create and replace tables
    * merges in the data -- automatically handles data duplication
-[] Cleanup metadata within airflow postgres database
    * Logs table
    * Xcom table
    * variables table
    * Task runs table
    1. PostgresHook - connect and run sql inside
    2. BashOperators - `sudo -u postgres psql -c "CREATE DATABASE airflow;"`

### Class pseudo code (extract.py)
```python
class Log_Cleanup():
    __init__(path, days):
        self.AGE_THRESHOLD = days

    # all the functions will go here

    execute(azure_blob, snowflake):
        # all the code you were using
        azure_blob.dump(df)
        snowflake.upload(etc)

```

### Dag pseudo code (cleanup_day.py)
```python
# Initialize classes
azure_blob = AzureBlob(initalizing_pieces_here)
snowflake = Snowflake(password, username, etc)

# Start cleanup logs
cleanup_logs = Log_Cleanup(base_path, days_threshold)
cleanup_logs.execute(azure_blob, snowflake)
```
