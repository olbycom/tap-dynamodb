import logging
import os
from typing import List

import boto3
import genson
import orjson
from botocore.exceptions import ClientError
from singer_sdk import typing as th  # JSON Schema typing helpers


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

    def list_tables(self, filters=None):
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
                yield response.get("Items", [])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except ClientError as err:
            self.logger.error(
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def get_key_properties(self, table_name):
        # TODO: use this for required
        dynamo_schema = self._client.Table(table_name).key_schema
        return [key.get("AttributeName") for key in dynamo_schema]

    def get_table_json_schema(self, table_name: str, strategy: str = "infer"):
        properties: List[th.Property] = []

        sample_records = list(
            self.get_items_iter(
                table_name, scan_kwargs={"Limit": 100, "ConsistentRead": True}
            )
        )[0]
        if strategy == "infer":
            builder = genson.SchemaBuilder(schema_uri=None)
            for record in sample_records:
                builder.add_object(
                    orjson.loads(
                        orjson.dumps(
                            record,
                            default=lambda o: str(o),
                            option=orjson.OPT_OMIT_MICROSECONDS,
                        ).decode("utf-8")
                    )
                )
            schema = builder.to_schema()
            self.recursively_drop_required(schema)
            if not schema:
                raise Exception("Inferring schema failed")
            self.logger.info("Inferred schema: %s", schema)
        else:
            raise Exception(f"Strategy {strategy} not supported")
        return schema

    @staticmethod
    def deserialize_record(data):
        deserializer = boto3.dynamodb.types.TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in data.items()}

    def recursively_drop_required(self, schema: dict) -> None:
        """Recursively drop the required property from a schema.

        This is used to clean up genson generated schemas which are strict by default.
        """
        schema.pop("required", None)
        if "properties" in schema:
            for prop in schema["properties"]:
                if schema["properties"][prop].get("type") == "object":
                    self.recursively_drop_required(schema["properties"][prop])
