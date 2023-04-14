"""DynamoDB tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.plugin_base import PluginBase

from tap_dynamodb import streams
from tap_dynamodb.connectors.aws_boto_connector import AWS_AUTH_CONFIG
from tap_dynamodb.dynamodb_connector import DynamoDbConnector
from tap_dynamodb.exception import EmptyTableException


class TapDynamoDB(Tap):
    """DynamoDB tap class."""

    name = "tap-dynamodb"

    config_jsonschema = th.PropertiesList(
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
        dynamodb_conn = DynamoDbConnector(
            self.config,
        )
        discovered_streams = []
        for table_name in self.config.get("tables") or dynamodb_conn.list_tables():
            try:
                stream = streams.TableStream(
                    tap=self,
                    name=table_name,
                    dynamodb_conn=dynamodb_conn,
                )
                discovered_streams.append(stream)
            except EmptyTableException:
                self.logger.warning(f"Skipping '{table_name}'. No records found.")
        return discovered_streams

    @classmethod
    def append_builtin_config(cls: type[PluginBase], config_jsonschema: dict) -> None:
        def _merge_missing(source_jsonschema: dict, target_jsonschema: dict) -> None:
            # Append any missing properties in the target with those from source.
            for k, v in source_jsonschema["properties"].items():
                if k not in target_jsonschema["properties"]:
                    target_jsonschema["properties"][k] = v

        _merge_missing(AWS_AUTH_CONFIG, config_jsonschema)
        super().append_builtin_config(config_jsonschema)


if __name__ == "__main__":
    TapDynamoDB.cli()
