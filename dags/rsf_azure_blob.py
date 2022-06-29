# pyright: reportMissingImports=false
# pyright: reportMissingModuleSource=false

import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from lib.rsf_interfaces import DumpInterface
from airflow.models import Variable
import csv
import os
from azure.storage.blob._shared.response_handlers import PartialBatchErrorException

def path_join(*args):
    return '/'.join(filter(None,args))

class AzureBlob(DumpInterface):

    def __init__(self, connection_string, container, sub_dir = None):
        self.connection_string = connection_string
        self.container = container
        self.sub_dir = sub_dir


    def clear(self, source_name, object_name, date = None):
        '''
        Checks Azure blob for current objects matching the same path name as the object to be created,
        then will delete those objects prior to uploading new versions.

                Parameters:
                    source_name (str): Name of source. Used for Azure parent folder
                    object_name (str): Name of object/table/endpoint used to create path
                    date (str, optional): Date of extract, defaults to None
                Returns:
                    None
        '''
        print(f'{source_name}/{object_name}')
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container = blob_service_client.get_container_client(self.container)

        blob_content = [blob['name'] for blob in container.list_blobs(name_starts_with=f'{source_name}/{object_name}/')]

        print('Clearing out old csvs')
        try:
            # [container.delete_blobs(blob) for blob in blob_content]
            for blob in blob_content:
                # print(blob)
                container.delete_blobs(blob)

        except PartialBatchErrorException as e:
            # azure.storage.blob._shared.response_handlers.PartialBatchErrorException: There is a partial failure in the batch operation.
            error = 'does not exist'
            if not error in str(e):
                raise e
            else:
                print('Partial failure in batch operation')


    def download(self, source_name, object_name):
        local_path = "./data"
        os.mkdir(local_path)

        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container = blob_service_client.get_container_client(self.container)

        blob = container.list_blobs(name_starts_with=f'{source_name}/{object_name}')

        print(blob['name'])

        blob_client = blob_service_client.get_blob_client(container=self.container, blob=f'{source_name}/{object_name}/{object_name}.csv')

        download_file_path = os.path.join(local_path, 'donwload_' + f'{source_name}/{object_name}/{object_name}.csv')
        print("\nDownloading blob to \n\t" + download_file_path)

        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
            
    
    def dump(self, df, source_name, object_name, date = None, suffix = ''):
        '''
        Uploads a pandas dataframe to Azure Blob with the name provided.
        The file path looks like {source_name}/{object_name}/{date when provided}/{object_name}.csv

                Parameters:
                        df (dataframe): pandas dataframe to be uploaded as a csv
                        source_name (str): name of source. Used for Azure Blob parent folder
                        object_name (str): name of table/endpoint Used to build object path
                        date (str, optional): date of extract, defaults to None. When ommited, full load is assumed

                Returns:
                        None
        '''

        if date:
            object_path = f'{source_name}/{object_name}/{date}/{object_name}{suffix}.csv'
        else:
            object_path = f'{source_name}/{object_name}/{object_name}{suffix}.csv'

        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = blob_service_client.get_blob_client(container=self.container, blob=object_path)

        csv_buffer = io.StringIO()
        # TO DO: figure out how to split csv upload by file size or row count
        try:
            df.to_csv(csv_buffer, index = False, quoting=csv.QUOTE_ALL)
            print('Size: ', df.shape, ', ', df.memory_usage(deep=True).sum())

            object_path = path_join(self.sub_dir,object_path)
            print("Uploading to: ", object_path)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        except Exception as e:
            print(f'Azure Blob upload failed due to {e}')
            raise e