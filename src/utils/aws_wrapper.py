import boto3
from boto3.exceptions import ResourceNotExistsError
import requests
from botocore.exceptions import ClientError
from oauthlib.oauth2 import Client

from utils.config_wrapper import ConfigWrapper
import tkinter as tk
from tkinter import messagebox, Scrollbar, Listbox, END

class AWS(ConfigWrapper):
    environment = None
    region = "us-east-1"
    token = None
    account_id = None
    role_name = None
    key_prefix = None
    max_session_duration = 129600
    session = None

    def __init__(self):
        super().__init__()
        self.environment = self.getenv("Environment").lower().strip()
        self.region = self.getenv("AWS_REGION")
        self.token = self.getenv("API_Access_Token")
        self.account_id = self.getenv(f"{self.environment}_AWS_ACCOUNT_ID")
        self.role_name = self.getenv(f"{self.environment}_AWS_ROLE_NAME")
        self.key_prefix = self.environment.upper()
        self.credentials_url = "https://portal.sso.us-east-1.amazonaws.com/federation/credentials"

        self.session = boto3.Session(
            aws_access_key_id=self.getenv(f"{self.environment.upper()}_AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=self.getenv(f"{self.environment.upper()}_AWS_SECRET_ACCESS_KEY"),
            aws_session_token=self.getenv(f"{self.environment.upper()}_AWS_SESSION_TOKEN"),
            region_name=self.region
        )
        self.sts = self.session.client('sts')

    def get_caller_identity(self):
        print(self.sts.get_caller_identity())


class AWSApi(AWS):
    def __init__(self):
        super().__init__()

    def generate(self):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }

        params = {
            "account_id": self.account_id,
            "role_name": self.role_name,
            "max-session-duration": self.max_session_duration
        }

        print("Calling AWS SSO Federation API...")
        response = requests.get(self.credentials_url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Failed to access AWS SSO API: {response.status_code}")
            print(response.text)
            return response.status_code

        print(f"API Successfully Accessed {response.status_code}")
        response_data = response.json()

        creds = response_data.get("roleCredentials", {})

        self.setenv(f"{self.key_prefix}_AWS_ACCESS_KEY_ID", creds["accessKeyId"])
        self.setenv(f"{self.key_prefix}_AWS_SECRET_ACCESS_KEY", creds["secretAccessKey"])
        self.setenv(f"{self.key_prefix}_AWS_SESSION_TOKEN", creds["sessionToken"])

        return response.status_code, creds

class AWSS3(AWS):
    s3_client = None
    def __init__(self):
        super().__init__()
        self.s3_client = self.session.client("s3")

    def bucket_exists(self, bucket_name):
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except:
            return False

    def load_s3_bucket(self, bucket):
        bucket_name = f"dlx-ddm-land-{self.environment}"
        prefix = f"consumer/{bucket}"

        # Get the bucket location and region info
        location = self.s3_client.get_bucket_location(Bucket=bucket_name).get("LocationConstraint", self.region)
        print(f"AWS Location Details: {location}")
        print(f"AWS Region Name Details: {self.session.region_name}")

        print(f"Objects in S3 bucket {bucket_name}/{prefix}:")
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if "Contents" in response:
            return response["Contents"]
        else:
            return None

    def list_buckets(self):
        buckets = self.s3_client.list_buckets()
        return [bucket['Name'] for bucket in buckets['Buckets']]

    def list_bucket_objects(self, bucket_name, prefix=None):
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response:
            return response["Contents"]
        else:
            return None

    def upload_file(self, bucket_name, file_path, object_name):
        self.s3_client.upload_file(bucket_name, file_path, object_name)

    def upload_content(self, bucket_name, object_name, content):
        self.s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=content)

    def download_file(self, bucket_name, object_name, destination):
        self.s3_client.download_file(bucket_name, object_name, destination)

    def list_objects(self, bucket_name, prefix=''):
        objects_list = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [obj['Key'] for obj in objects_list.get('Contents', [])]

class AWSDynamoDB(AWS):
    def __init__(self):
        super().__init__()
        self.dynamodb = self.session.client("dynamodb", region_name=self.region)
        self.dynamodb_resource = boto3.resource('dynamodb', region_name=self.region)

    def list_tables(self):
        return self.dynamodb.list_tables()['TableNames']

    def table_exists(self, table_name):
        try:
            self.dynamodb.describe_table(TableName=table_name)
            return True
        except Exception as e:
            return False

    def table_info(self, table_name):
        return self.dynamodb.describe_table(TableName=table_name)

    def deserialize_dynamodb_item(self, item):
        """
        Recursively converts a DynamoDB item (from client.get_item or scan)
        to a native Python dictionary.
        """

        if not isinstance(item, dict):
            return item
        # If it's a full item: { "attr": { "S": "value" }, ... }
        if all(isinstance(v, dict) and len(v) == 1 for v in item.values()):
            return {k: self.deserialize_dynamodb_item(v) for k, v in item.items()}

        # Each DynamoDB item has a single type key like {'S': 'value'}, {'M': {...}}, {'L': [...]}
        for key, value in item.items():
            if key == 'S':
                return value
            elif key == 'N':
                return int(value) if value.isdigit() else float(value)
            elif key == 'BOOL':
                return value
            elif key == 'NULL':
                return None
            elif key == 'L':
                return [self.deserialize_dynamodb_item(i) for i in value]
            elif key == 'M':
                return {k: self.deserialize_dynamodb_item(v) for k, v in value.items()}
            elif key == 'SS':
                return value
            elif key == 'NS':
                return [int(n) if n.isdigit() else float(n) for n in value]
            else:
                return value
        return None

    def get_item(self, table_name, query):
        try:
            response = self.dynamodb.get_item(
                TableName=table_name,
                Key=query
            )
            if 'Item' in response:
                return self.deserialize_dynamodb_item(response['Item'])
                # return {k: list(v.values())[0] for k, v in response['Item'].items()}
            else:
                return {}
        except ClientError as e:
            return f'Getting error: {e.response['Error']['Message']}'

class AWSCloudwatch(AWS):
    def __init__(self):
        super().__init__()
        self.cloudwatch = self.session.client("logs",region_name=self.region)

    def list_groups(self):
        paginator = self.cloudwatch.get_paginator('describe_log_groups')
        log_groups = []
        for page in paginator.paginate():
            for group in page['logGroups']:
                log_groups.append(group['logGroupName'])

        return log_groups

    def pull_logs(self, log_group, filter_keywords):
        response = self.cloudwatch.filter_log_events(
            logGroupName=log_group,
            filterPattern=filter_keywords
        )
        return response['events']

    def pull_logs_stream(self, log_group, log_stream_name):
        response = self.cloudwatch.filter_log_events(
            logGroupName=log_group,
            logStreamNames=[log_stream_name]
            # filterPattern="jr_49e04d0bde26"
        )
        return response['events']

if __name__ == "__main__":
    # file_chooser = AWSS3FileChooser()
    # bucket_name = "dlx-ddm-process-dev"
    # path_prefix = "consumer/"
    # file_chooser.create_widgets()
    # file_chooser.load_s3_files(bucket_name, path_prefix)
    # file_chooser.mainloop()
    # obj = AWSDynamoDB()
    # response = obj.get_item('ddm_client_trigger_config', 'travis-consumer_trigger')
    # print(response)
    cw = AWSCloudwatch()
    log_group = '/aws-glue/jobs/logs-v2'
    log_stream_name = 'jr_50dd83e466f4fe20fb8d8132c50bf3181b0a31c76b8c1af8a697904120ee1c3b'
    logs = cw.pull_logs_stream(log_group, log_stream_name)
    print(logs)
