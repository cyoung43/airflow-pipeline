class DumpInterface:
    """Interface for dumping pandas dataframes. Classes that will inherit this are s3, azure blob, etc.
    """   
    def clear(self, source_name, object_name, date = None):
        '''Clears previous blobs from specified blob prefix
        
            Args:
                source_name (str): name of the source. Used in path for prefix
                object_name (str): full path to the data (minus the source name)
                date (date, optional): for incremental loads, defaults to none
        '''

        pass

    def dump(self, df, source_name, object_name, date = None, suffix=''):
        """Uploads a dataframe to the data store

        Args:
            df (dataframe): pandas dataframe
            source_name (str): name of the source. Used in path for data
            object_name (str): full path to the data (minus the source name)
            date (date, optional): For incremental Loads. Defaults to None.
        """        
        pass

class LoadInterface:
    def create_target_table(self, table_name, columns, data_types, replace = True, schema = 'DATA_LAKE'):
        """Creates or recreates a target table using pandas column metadata

        Args:
            table_name (str): table name to be created
            columns (list): pandas dataframe list (df.columns.tolist())
            data_types (dict): pandas data type dictionary
            replace (bool, optional): When true it will treat the table as full load and drop and recreate it. Defaults to True.
            schema (str, optional): Schema to insert the data. Defaults to 'DATA_LAKE'.
        """        
        pass
    def full_load(self, source_name, table_name, columns, date = None, schema = 'DATA_LAKE', time_stamp_format = None):
        """Loads data into the database. Initially designed for loading data into snowflake from a object store like s3 or azure blob

        Args:
            source_name (str): name of the source, used for first level folder in s3 or blob
            table_name (str): table name to insert
            columns (list): pandas dataframe columns as a list
            date (date, optional): When provided, the date is used to target a specific folder in the stage. Defaults to None.
            schema (str, optional): database schema to load the data. Defaults to 'DATA_LAKE'.
        """        
        pass
    def incremental_load(self, source_name, table_name, columns, primary_key_columns: list, incremental_column, date, schema = 'DATA_LAKE'):
        """Using a merge statement, incrementally load data from the stage

        Args:
            source_name (str): [description]
            table_name ([type]): [description]
            columns ([type]): [description]
            primary_key_columns (list): list of primary key columns to use for the merge on statement
            incremental_column (str): the name of the incremental date column to use in the order by of the de-duplication qualify statement
            date (date): Used for the path to the incremental file
            schema (str, optional): target table schema. Defaults to 'DATA_LAKE'.
        """        
        pass

    def merge(self, source_name, table_name, columns, primary_key_columns: list, incremental_column = None, schema = 'DATA_LAKE'):
        """Use merge statement explicitly to assist load of table
        
        Args:
            source_name (str): name of the source, used for first level folder/directory in s3 or blob
            table_name (str): table name to insert
            columns (list): pandas dataframe columns as a list
            primary_key_columns (list): list of primary key columns to use for the merge on statement
            incremental_column (str, optional): the name of the incremental date column to use order by. Default is None
            schema (str, optional): target table schema. Defaults to 'DATA_LAKE'
        """

class ApiInterface:
    def extract(self, dump_obj, endpoint_name, ti):
        """Extracts data from the data store into a pandas dataframe

        Args:
            dump_obj (EbDumpInterface): object that will be used to dump the data
            endpoint_name (str): name of the endpoint
            ti (TaskInstance): task instance
        """        
        pass

    def create_table(self, endpoint, replace, load_obj, ti, group_task=False, full_name=None):
        """Creates or recreates a target table using pandas column metadata

        Args:
            item (str): Endpoint/table name to be created
            replace (boolean): boolean value to determine if table is to be replaced. Should be False for incremental/merging
            load_obj (EbLoadInterface): object that will be used to load the data
            ti (TaskInstance): task instance
            group_task (boolean, optional): boolean value to determine if the task is a group task. Defaults to False.
            full_name (str, optional): full name of the table. Defaults to None.
        """        
        pass

    def load(self, endpoint, load_obj, ti, group_task=False, full_name=None):
        """Loads data from staging into Snowflake

            Args:
                item (str): Endpoint/table name to be created
                load_obj (EbLoadInterface): object that will be used to load the data
                ti (TaskInstance): task instance
                group_task (boolean, optional): boolean value to determine if the task is a group task. Defaults to False.
                full_name (str, optional): full name of the table. Defaults to None.
        """        
        pass