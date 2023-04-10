"""DynamoDB tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_dynamodb import streams
from tap_dynamodb.dynamo import DynamoDB


class TapDynamoDB(Tap):
    """DynamoDB tap class."""

    name = "tap-dynamodb"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> list[streams.DynamoDBStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        obj = DynamoDB()
        obj.authenticate(self.config)
        return [
            streams.TableStream(
                tap=self,
                name=table_name,
                dynamodb_obj=obj,
            )
            for table_name in obj.list_tables(filters="TODO - filters")
        ]


if __name__ == "__main__":
    TapDynamoDB.cli()
