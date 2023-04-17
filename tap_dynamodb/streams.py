"""Stream type classes for tap-dynamodb."""

from __future__ import annotations

from typing import Iterable

from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams import Stream

from tap_dynamodb.dynamodb_connector import DynamoDbConnector


class TableStream(Stream):
    """Stream class for TableStream streams."""

    def __init__(
        self,
        tap: TapBaseClass,
        name: str,
        dynamodb_conn: DynamoDbConnector,
        infer_schema_sample_size,
    ):
        """
        Initialize a new TableStream object.

        Args:
            tap: The parent tap object.
            name: The name of the stream.
            dynamodb_conn: The DynamoDbConnector object.
            infer_schema_sample_size: The amount of records to sample when
                inferring the schema.
        """
        self._dynamodb_conn: DynamoDbConnector = dynamodb_conn
        self._table_name: str = name
        self._schema: dict = {}
        self._infer_schema_sample_size = infer_schema_sample_size
        super().__init__(
            tap=tap,
            schema=self.schema,
            name=name,
        )

    def get_records(self, context: dict | None) -> Iterable[dict]:
        for batch in self._dynamodb_conn.get_items_iter(self._table_name):
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
            self._schema = self._dynamodb_conn.get_table_json_schema(
                self._table_name,
                self._infer_schema_sample_size,
            )
            self.primary_keys = self._dynamodb_conn.get_table_key_properties(
                self._table_name
            )
        return self._schema
