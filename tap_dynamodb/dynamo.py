import logging
import os

import boto3
from botocore.exceptions import ClientError


class DynamoDB:
    def __init__(self):
        self._client = None
        self.logger = logging.getLogger(__name__)

    def authenticate(self, config):
        if self._client:
            return

        aws_access_key_id = config.get("aws_access_key_id") or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        aws_secret_access_key = config.get("aws_secret_access_key") or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        aws_session_token = config.get("aws_session_token") or os.environ.get(
            "AWS_SESSION_TOKEN"
        )
        aws_profile = config.get("aws_profile") or os.environ.get("AWS_PROFILE")
        aws_endpoint_url = config.get("aws_endpoint_url")
        aws_region_name = config.get("aws_region_name")

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region_name,
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)
        if aws_endpoint_url:
            client = aws_session.resource("dynamodb", endpoint_url=aws_endpoint_url)
        else:
            client = aws_session.resource("dynamodb")
        self._client = client

    def list_tables(self):
        try:
            tables = []
            for table in self._client.tables.all():
                tables.append(table)
        except ClientError as err:
            self.logger.error(
                "Couldn't list tables. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return tables

    def get_items_iter(
        self, table_name: str, scan_kwargs: dict = {"ConsistentRead": True}
    ):
        table = self._client.Table(table_name)
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = table.scan(**scan_kwargs)
                yield [self.deserialize_record(item) for item in response.get("Items", [])]
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except ClientError as err:
            self.logger.error(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def get_table_json_schema(
            self,
            table_name: str,
            strategy: str = "infer"
    ):
        raise NotImplementedError

    @staticmethod
    def deserialize_record(data):
        deserializer = boto3.dynamodb.types.TypeDeserializer()
        return {k: deserializer.deserialize(v) for k,v in data.items()}
