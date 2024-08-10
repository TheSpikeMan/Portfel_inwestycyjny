from google.cloud import storage

def list_blobs(bucket_name):
    """
    Lists all the blobs (files) in the bucket

    bucket_name: STRING - name of the bucket
    
    """
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        print(blob.name)


def read_sql_from_bucket(bucket_name, user_project, file_name):
    """
    Read sql from specific filename from bucket

    bucket_name:  STRING - name of the bucket
    user_project: STRING - project name
    file_name   : STRING - file name with extension

    """
    storage_client = storage.Client()

    # Creating a Bucket Class Object
    bucket = storage_client.bucket(bucket_name=bucket_name, user_project=user_project)

    # Reading a specific filename from bucket
    blob_object = bucket.blob(blob_name=file_name)

    # Downloading a blob object as 'bytes' object
    blob_as_bytes = blob_object.download_as_bytes()

    # Decoding the 'bytes' object to string
    blob = blob_as_bytes.decode('UTF-8')

    return blob
