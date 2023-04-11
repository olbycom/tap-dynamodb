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
        self.table_name = kwargs.get("name")
        self.dynamodb_obj: DynamoDB = kwargs.pop("dynamodb_obj")
        self._schema = None
        super().__init__(*args, **kwargs)

    def get_jsonschema_type(self):
        return "string"

    def get_records(self, context: dict | None) -> Iterable[dict]:
        for batch in self.dynamodb_obj.get_items_iter(self.table_name):
            for record in batch:
                yield record

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.

        Returns:
            dict
        """
        # TODO: SDC columns
        if not self._schema:
            self._schema = self.dynamodb_obj.get_table_json_schema(self.table_name)
            self.primary_keys = self.dynamodb_obj.get_table_key_properties(
                self.table_name
            )
        return self._schema
