"""DynamoDB tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_dynamodb import streams
from tap_dynamodb.dynamo import DynamoDB
from tap_dynamodb.exception import EmptyTableException


class TapDynamoDB(Tap):
    """DynamoDB tap class."""

    name = "tap-dynamodb"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "aws_access_key_id",
            th.StringType,
            secret=True,
            description="The access key for your AWS account.",
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            secret=True,
            description="The secret key for your AWS account.",
        ),
        th.Property(
            "aws_session_token",
            th.StringType,
            secret=True,
            description=(
                "The session key for your AWS account. This is only needed when"
                " you are using temporary credentials."
            ),
        ),
        th.Property(
            "aws_profile",
            th.StringType,
            description=(
                "The AWS credentials profile name to use. The profile must be "
                "configured and accessible."
            ),
        ),
        th.Property(
            "aws_default_region",
            th.StringType,
            description="The default AWS region name (e.g. us-east-1) ",
        ),
        th.Property(
            "aws_endpoint_url",
            th.StringType,
            description="The complete URL to use for the constructed client.",
        ),
        th.Property(
            "aws_assume_role_arn",
            th.StringType,
            description="The role ARN to assume.",
        ),
        th.Property(
            "use_aws_env_vars",
            th.BooleanType,
            default=False,
            description=(
                "Whether to retrieve aws credentials from environment variables."
            ),
        ),
        th.Property(
            "tables",
            th.ArrayType(th.StringType),
            description="An array of table names to extract from.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TableStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        obj = DynamoDB(self.config)
        discovered_streams = []
        for table_name in obj.list_tables(self.config.get("tables")):
            try:
                stream = streams.TableStream(
                    tap=self,
                    name=table_name,
                    dynamodb_obj=obj,
                )
                discovered_streams.append(stream)
            except EmptyTableException:
                self.logger.warning(f"Skipping '{table_name}'. No records found.")
        return discovered_streams


if __name__ == "__main__":
    TapDynamoDB.cli()
