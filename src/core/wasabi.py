import os
import boto3
import json


class WASABI_CONNECT:
    """
    connect to our wasabi bucket
    """

    def __init__(
        self,
        service_endpoint: str,
        access_key_id: str,
        secret_access_key: str,
        bucket_name: str = "ai4good-diversity",
    ):
        self.service_endpoint = service_endpoint
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket_name = bucket_name

    def object_content(self, object_path: str = "company_scorecards/scorecards.json"):
        """
        returns the content of a file stored in the bucket
        """
        wasa_resources = boto3.resource('s3',
                                        aws_access_key_id=self.access_key_id,
                                        aws_secret_access_key=self.secret_access_key,
                                        endpoint_url=self.service_endpoint)
        wasa_object = wasa_resources.Object(self.bucket_name, object_path)
        file_content = wasa_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content

    def list_of_files_in_folder(self, folder_path: str = "company_scorecards/"):
        """
        """
        wasa_resources = boto3.resource('s3',
                                        aws_access_key_id=self.access_key_id,
                                        aws_secret_access_key=self.secret_access_key,
                                        endpoint_url=self.service_endpoint)
        wasa_bucket = wasa_resources.Bucket(self.bucket_name)
        object_list = []
        for object_summary in wasa_bucket.objects.filter(Prefix=folder_path):
            object_list.append[object_summary.key]
        return object_list
