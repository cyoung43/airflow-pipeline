import snowflake.connector as sc
from sqlalchemy import column, table, true
from rsf_interfaces import LoadInterface

def convert_dtype(dtype):
    '''
    Returns the snowflake datatype based on the pandas dataframe datatype.

            Parameters:
                    dtype (str): pandas datatype

            Returns:
                    dtype (str): snowflake datatype
    '''
    # print("dtype: ", dtype)

    result = "text"
    if dtype == 'object' or dtype == 'bool':
        result = 'text'
    elif dtype == 'int64':
        result = 'int'
    elif dtype == 'float64':
        result = "number"
    elif 'datetime64' in dtype:
        result = 'datetime'

    return result

def get_column_string(columns):
    return ','.join("nullif(${}, '') as {}".format(key+1,value) for key, value in enumerate(columns))


def get_column_object(columns, data_types = False):
    """Check anticipated snowflake columns to see if any column names are conflicting with snowflake reserved words.

        Parameters:
            columns (list/dict): list or dictionary of words to check
            data_types (bool, optional): When true, the column object is a dictionary, which drops into the if block to change the logic

        Returns:
            new_cols (list/dict): list or dictionary of the column names/data types
    
    """
    # Add keywords here as errors appear on snowflake runs
    keywords = ['start', 'end', 'display_name', 'group']

    new_cols = {}

    # Add on endings to the keywords to avoid snowflake errors
    if data_types:
        for key, value in columns:
            if key in keywords:
                if key in ['group', 'display_name']:
                    new_cols[f'{key}_id'] = str(value)
                else:
                    new_cols[f'{key}_date'] = str(value)
            else:
                new_cols[key] = str(value)

    else:
        for col in columns:
            if col in keywords:
                if col in ['group', 'display_name']:
                    new_cols[f'{col}_id'] = f'{col}_id'
                else:
                    new_cols[f'{col}_date'] = f'{col}_date'
            else: 
                new_cols[col] = col

    return new_cols


class Snowflake(LoadInterface):

    def __init__(self, account, database, warehouse, user, password, stage_name, schema = 'DATA_LAKE', file_format = None):
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.user = user
        self.password = password
        self.stage_name = stage_name
        self.schema = schema
        self.file_format = file_format
        self.conn = None

    def connect(self):
        self.conn = sc.connect(user = self.user, password = self.password, account = self.account)

    def get_cursor(self):
        if not self.conn:
            self.connect()
        cur = self.conn.cursor()
        cur.execute('USE DATABASE %s;' % self.database)
        cur.execute('USE WAREHOUSE %s;' % self.warehouse)

        return cur

    def get_results(self, sql): 
        cur = self.get_cursor()
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()
        return results


    def create_target_table(self, table_name, columns, data_types, replace, schema = 'DATA_LAKE'):
        """Creates a table in snowflake based on pandas dataframe metadata.

        Args:
            table_name (str): the table name to be created
            columns (list): the output of pandas df.columns.tolist()
            data_types ([type]): the output of pandas dict(df.dtypes)
            replace (bool, optional): Whether to run a create or replace or a create if not exists. Defaults to True.
            schema (str, optional): the schema to create the table in. Defaults to 'DATA_LAKE'.
        """        
        column_string = '\n,'.join(map(lambda column: column + ' ' + convert_dtype(data_types[column]), columns))

        print(column_string)

        if replace:
            create_string = 'CREATE OR REPLACE TABLE'
        else:
            create_string = 'CREATE TABLE IF NOT EXISTS'

        query = f'''
        {create_string} {schema}.{table_name} (
            {column_string}
            ,DATA_LAKE_INSERT_DATE timestamp_ntz not null default convert_timezone('UTC', current_timestamp())::timestamp_ntz
        )
        '''

        print("Creating Table: ", query)
        cur = self.get_cursor()
        cur.execute(query)
        cur.close()
    


    def full_load(self, source_name, table_name, columns, date = None, schema = 'DATA_LAKE', time_stamp_format = None):
        
        source_column_string = get_column_string(columns)

        if date: # Must match structure in eb_s3.py
            object_path = f'{source_name}/{table_name}/{date}/{table_name}'
        else:
            object_path = f'{source_name}/{table_name}/{table_name}'
        
        query = f'''
        INSERT OVERWRITE INTO {schema}.{table_name}
        SELECT
            {source_column_string}
            ,convert_timezone('UTC', current_timestamp())::timestamp_ntz as DATA_LAKE_INSERT_DATE
        FROM @{self.stage_name}/{object_path} {f'(file_format => {self.file_format})' if self.file_format else ''}
        '''

        print("Insert Query: ", query)
        cur = self.get_cursor()
        if time_stamp_format:
            cur.execute(f"ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = '{time_stamp_format}'") ## This is required for snowflake to correctly parse the timestamps we recieve from mysql
        cur.execute(query)
        cur.close()


    def merge(self, source_name, table_name, columns, primary_key_columns: list, incremental = False, date = None, incremental_column = None, schema = 'DATA_LAKE'):
        object_path = f'{source_name}/{table_name}/{table_name}'
        
        if incremental:
            object_path = f'{source_name}/{table_name}/{date}/{table_name}'
        
        source_column_string = get_column_string(columns)

        on_clause = '\nAND '.join(map(lambda column: f's.{column} = t.{column} ', primary_key_columns))
        non_primary_columns = [column for column in columns if column not in primary_key_columns]

        set_clause = ',\n'.join(map(lambda column: f't.{column} = s.{column}', non_primary_columns))
        insert_values_clause = ','.join(map(lambda column: f's.{column}', columns))

        incremental_statement = f'qualify row_number() over (partition by {",".join(primary_key_columns)} order by {incremental_column} desc) = 1'

        query = f"""
            merge into {schema}.{table_name} t
            using
                (select distinct {source_column_string}
                from @{self.stage_name}/{object_path} {f'(file_format => {self.file_format})' if self.file_format else ''}
                {incremental_statement if incremental else ''}
                ) s
            on {on_clause}
            when matched then update set
                {set_clause}
            when not matched then insert({','.join(columns)}) values ({insert_values_clause})
        """

        print('Insert query', query)
        cur = self.get_cursor()
        cur.execute(query)

        results = cur.fetchall()
        print('New records, updated records:', results)
        
        cur.close()
        

    def incremental_load(self, source_name, table_name, columns, primary_key_columns: list, incremental_column, date, schema = 'DATA_LAKE'):
        """This function enables a daily incremental load and calls the merge function.

        Args:
            source_name (str): the name of the data source
            table_name (str): the name of the table to be loaded
            columns (list): the values of columns found in xcom
            primary_key_columns (list): specifies the primary key columns
            incremental_column (str): the column to be used for incremental load
            date (str): the date to be used for incremental load
            schema (str, optional): the schema to create the table in. Defaults to 'DATA_LAKE'.
        """

        self.merge(source_name, table_name, columns, primary_key_columns, True, date, incremental_column, schema)
        







