import os

# Import the client object from the SDK library
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient,PublicAccess

# Retrieve the connection string from an environment variable. Note that a connection
# string grants all permissions to the caller, making it less secure than obtaining a
# BlobClient object using credentials.
conn_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
container_name="blob-container-01"

#define de 
path_remove = "D:\\"
local_path = "data"


# Create the client object for the resource identified by the connection string,
# indicating also the blob container and the name of the specific blob we want.
service_client=BlobServiceClient.from_connection_string(conn_string)
container_client = service_client.get_container_client(container_name)


# Open a local file and upload its contents to Blob Storage
for r,d,f in os.walk(local_path):
    if f:
        for file in f:
            file_path_on_azure = os.path.join(r,file).replace(path_remove,"")
            file_path_on_local = os.path.join(r,file)
            blob_client = container_client.get_blob_client(file_path_on_azure)
            with open(file_path_on_local,'rb') as data:
                blob_client.upload_blob(data)







