import boto3
import json
import gzip
import os

SERVICE_ENDPOINT = os.getenv(
    "SERVICE_ENDPOINT", "https://s3.us-west-1.wasabisys.com")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")


def list_files_folders(
    service_endpoint=SERVICE_ENDPOINT,
    access_key_id=ACCESS_KEY_ID,
    secret_access_key=SECRET_ACCESS_KEY,
    bucket_name="dei-bucket",
):
    """
    lists all files and folders in the specified Wasabi bucket
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      endpoint_url=service_endpoint)

    response = s3.list_objects_v2(Bucket=bucket_name)

    files = []
    folders = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('/'):
            folders.append(obj['Key'])
        else:
            files.append(obj['Key'])

    return (files, folders)


def read_jsonl_file(
    service_endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str = "dei-bucket",
    object_path: str = "raw_data/ADM.jsonl.gz",
    return_lines: bool = False,
):
    """
    reads the contents of a JSON Lines file stored in a Wasabi bucket
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      endpoint_url=service_endpoint)

    response = s3.get_object(Bucket=bucket_name, Key=object_path)

    # read the contents of the file
    file_content = response['Body'].read()

    # decompress the gzipped content
    file_content = gzip.decompress(file_content)

    # parse the JSON lines
    json_lines = file_content.decode('utf-8').split('\n')
    if return_lines:
        return json_lines
    else:
        json_objects = []
        for line in json_lines:
            if line:
                json_objects.append(json.loads(line))

        return json_objects


def download_and_save_jsonl_file_locally(
    local_path: str,
    service_endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str = "dei-bucket",
    object_path: str = "raw_data/ADM.jsonl.gz"
):
    """
    Downloads a JSON Lines file from S3 and saves it locally as a .jsonl.gz file
    """
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      endpoint_url=service_endpoint)

    response = s3.get_object(Bucket=bucket_name, Key=object_path)

    # read the contents of the file
    file_content = response['Body'].read()

    # decompress the gzipped content
    file_content = gzip.decompress(file_content)

    # parse the JSON lines
    json_lines = file_content.decode('utf-8').split('\n')
    json_objects = []
    for line in json_lines:
        if line:
            json_objects.append(json.loads(line))

    # save the JSON objects to a new file
    with gzip.open(local_path, 'wb') as f:
        for obj in json_objects:
            f.write(json.dumps(obj).encode('utf-8'))
            f.write('\n'.encode('utf-8'))


def create_s3_folder(
    service_endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str,
    folder_path: str
):
    """
    Creates a new folder (prefix) in an S3 bucket
    """
    # ensure the folder path ends with a forward slash
    if not folder_path.endswith('/'):
        folder_path += '/'

    # create a new object in the bucket with the folder path as the key
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      endpoint_url=service_endpoint)
    s3.put_object(Bucket=bucket_name, Key=(folder_path))


def save_dict_to_s3_as_jsonl_file(
    data_dict: dict,
    service_endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str = "dei-bucket",
    object_path: str = "raw_data/ADM.jsonl.gz"
):
    """
    Takes a Python dictionary, converts it to JSON Lines format, and saves it to a specified location in a S3 bucket
    """
    # convert dictionary to JSON Lines format
    json_lines = []
    for key, value in data_dict.items():
        json_lines.append(json.dumps({key: value}))

    # compress the JSON Lines content
    file_content = gzip.compress('\n'.join(json_lines).encode('utf-8'))

    # upload the compressed content to S3
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      endpoint_url=service_endpoint)
    s3.put_object(Body=file_content, Bucket=bucket_name, Key=object_path)
