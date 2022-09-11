from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from dotenv import load_dotenv
import os

load_dotenv()

local_path = "./data"
if not os.path.exists(local_path):
    os.mkdir(local_path)

c_string = os.getenv('CONNECTION_STRING')
c_name = os.getenv('CONTAINER_NAME')
a_name = os.getenv('ACCOUNT_NAME')


def download_files(connection_string, container_name, blob_target):
    try:
        container_client = ContainerClient.from_connection_string(connection_string, container_name)

        blob_list = container_client.list_blobs()

        for blob in blob_list:
            str_blob_name = str(blob.name)
            if '.csv' and blob_target in str_blob_name:
                blob_name = str_blob_name

                slice_b_name = blob_name.split('/')

                local_file_name = slice_b_name[1]

                blob_service_client = BlobServiceClient.from_connection_string(connection_string)

                blob_client = blob_service_client.get_container_client(container_name)

                download_file_path = os.path.join(local_path, local_file_name)

                with open(download_file_path, "wb") as download_file:
                    download_file.write(blob_client.download_blob(blob_name).readall())

                blob_url = f"https://{a_name}.blob.core.windows.net/{container_name}/{blob_name}"

                target_file = f"procesados/{local_file_name}"

                copied_blob = blob_service_client.get_blob_client(container_name, target_file)

                copy = copied_blob.start_copy_from_url(blob_url)

                props = copied_blob.get_blob_properties()

                copy_id = props.copy.id

                if props.copy.status != "success":
                    copied_blob.abort_copy(copy_id)

                remove_blob = blob_service_client.get_blob_client(container_name, blob_name)

                remove_blob.delete_blob()

                return True
    except:
        return False


async def download_async_files(connection_string, container_name,  blob_target):
    try:
        container_client = ContainerClient.from_connection_string(connection_string, container_name)

        blob_list = container_client.list_blobs()

        for blob in blob_list:
            str_blob_name = str(blob.name)
            if '.csv' and blob_target in str_blob_name:
                blob_name = str_blob_name

                slice_b_name = blob_name.split('/')

                local_file_name = slice_b_name[1]

                blob_service_client = BlobServiceClient.from_connection_string(connection_string)

                blob_client = blob_service_client.get_container_client(container_name)

                download_file_path = os.path.join(local_path, local_file_name)

                with open(download_file_path, "wb") as download_file:
                    stream = await blob_client.download_blob(blob_name)
                    data = await stream.readall()
                    download_file.write(data)

                blob_url = f"https://{a_name}.blob.core.windows.net/{container_name}/{blob_name}"

                target_file = f"procesados/{local_file_name}"

                copied_blob = blob_service_client.get_blob_client(container_name, target_file)

                copy = copied_blob.start_copy_from_url(blob_url)

                props = copied_blob.get_blob_properties()

                copy_id = props.copy.id

                if props.copy.status != "success":
                    copied_blob.abort_copy(copy_id)

                remove_blob = blob_service_client.get_blob_client(container_name, blob_name)

                remove_blob.delete_blob()
                return True
    except:
        return False
