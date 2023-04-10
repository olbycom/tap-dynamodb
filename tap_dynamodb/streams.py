"""Stream type classes for tap-dynamodb."""

from __future__ import annotations

from typing import Iterable

from singer_sdk.streams import Stream

from tap_dynamodb.dynamo import DynamoDB


class TableStream(Stream):
    """Stream class for TableStream streams."""

    def __init__(self, *args, **kwargs):
        """
        Initialize a new TableStream object.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
                table_name (str): Name of the DynamoDB table to stream.
                dynamodb_obj (DynamoDB): Instance of the DynamoDB client.

        """
        self.table_name = kwargs.get("table_name")
        self.dynamodb_obj: DynamoDB = kwargs.get("dynamodb_obj")
        super().__init__(*args, **kwargs)

    def get_jsonschema_type(self):
        return "string"

    def get_records(self, context: dict | None) -> Iterable[dict]:
        yield from self.dynamodb_obj.get_items_iter(self.table_name)

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.

        Returns:
            dict
        """
        # TODO: SDC columns
        return self.dynamodb_obj.get_table_json_schema(self.table_name)
