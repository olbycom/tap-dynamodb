"""Stream type classes for tap-dynamodb."""

from __future__ import annotations

import datetime
import hashlib
import json
import re
import sys
import typing as t
from decimal import Decimal
from functools import cached_property

from boto3.dynamodb.types import TypeDeserializer
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.streams import Stream
from singer_sdk import typing as th

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from nekt_singer_sdk.helpers.types import Context
    from nekt_singer_sdk.tap_base import Tap

    from tap_dynamodb.dynamodb_connector import DynamoDbConnector


_ISO8601_DATE_PATTERN = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})(?P<rest>T.*)?$")


def _shift_lookback(value: t.Any, lookback_days: int) -> t.Any | None:
    """Shift a replication-key starting value back by `lookback_days`.

    For strings, only the calendar date is recomputed; the time-of-day/fractional-
    seconds/timezone substring (if any) is spliced back verbatim rather than
    reparsed and reformatted. DynamoDB compares this value against the raw stored
    attribute lexicographically, so it must keep whatever precision/format the
    source table already uses — not reconstructing that substring at all is safer
    than trying to reproduce it. Numeric epoch values (seconds or milliseconds, by
    magnitude) are handled by plain arithmetic. Returns None if `value`'s type or
    format isn't recognized, leaving the decision to skip the lookback to the caller.
    """
    if isinstance(value, str):
        match = _ISO8601_DATE_PATTERN.match(value)
        if not match:
            return None
        try:
            shifted_date = datetime.date.fromisoformat(match.group("date")) - datetime.timedelta(days=lookback_days)
        except ValueError:
            return None
        return f"{shifted_date.isoformat()}{match.group('rest') or ''}"

    if isinstance(value, (int, float)):
        seconds = datetime.timedelta(days=lookback_days).total_seconds()
        is_millis = value >= 1_000_000_000_000
        return value - (seconds * 1000 if is_millis else seconds)

    return None


def _serialize_dynamodb_value(obj: t.Any) -> t.Any:
    """JSON serializer for DynamoDB types."""
    if isinstance(obj, Decimal):
        return int(obj) if obj == int(obj) else float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class TableStream(Stream):
    """Stream class for TableStream streams."""

    user_defined_replication_key = None

    def __init__(
        self,
        tap: Tap,
        name: str,
        dynamodb_conn: DynamoDbConnector,
        infer_schema_sample_size: int,
        replication_key: str | None,
        replication_method: str,
    ):
        """Initialize a new TableStream object.

        Args:
            tap: The parent tap object.
            name: The name of the stream.
            dynamodb_conn: The DynamoDbConnector object.
            infer_schema_sample_size: The amount of records to sample when
                inferring the schema.
            replication_key: The key to use for incremental replication.
            replication_method: The method to use for incremental replication.
        """
        self.user_defined_replication_key = replication_key
        self.user_defined_replication_method = replication_method
        self.deserializer = TypeDeserializer()

        self._dynamodb_conn: DynamoDbConnector = dynamodb_conn
        self._table_name: str = name
        self._schema: dict = {}
        self._infer_schema_sample_size = infer_schema_sample_size
        self._table_scan_kwargs: dict = tap.config.get("table_scan_kwargs", {}).get(name, {})
        if tap.input_catalog:
            catalog_entry = tap.input_catalog.get(name)
            if catalog_entry:
                super().__init__(
                    name=name,
                    tap=tap,
                    schema=catalog_entry.to_dict().get("schema"),
                )
            else:
                user_logger.error(
                    f"Catalog provided with selected table '{name}' missing. Either add the table to the catalog or remove it from the config."
                )
                sys.exit(1)
        else:
            super().__init__(name=name, tap=tap)

    @cached_property
    def dynamodb_primary_keys(self) -> list[str]:
        return self._dynamodb_conn.get_table_key_properties(self._table_name)

    @property
    def schema(self) -> dict:
        """Dynamically detect the json schema for the stream.

        This is evaluated prior to any records being retrieved.

        Returns:
            dict
        """
        if not self._schema:
            if self.config.get("extraction_mode") == "infer_schema":
                self._schema = self._dynamodb_conn.get_table_json_schema(
                    self._table_name,
                    self._infer_schema_sample_size,
                    self._table_scan_kwargs,
                )
                # Coerce the replication key to a datetime if it's a string
                if (
                    self.user_defined_replication_key
                    and self.user_defined_replication_key in self._schema["properties"]
                    and self._schema["properties"][self.user_defined_replication_key]["type"] == "string"
                ):
                    self._schema["properties"][self.user_defined_replication_key]["format"] = "date-time"

                self._primary_keys = self._dynamodb_conn.get_table_key_properties(self._table_name)
            elif self.config.get("extraction_mode") == "envelope":
                envelope_schema = th.PropertiesList(
                    th.Property("_hash_id", th.StringType),
                    th.Property("document", th.StringType),
                )

                if self.user_defined_replication_key:
                    envelope_schema.append(th.Property(self.user_defined_replication_key, th.DateTimeType))

                self._primary_keys = ["_hash_id"]
                self._schema = envelope_schema.to_dict()

            self._replication_key = self.user_defined_replication_key
            self._replication_method = self.user_defined_replication_method

            user_logger.info(f"[{self._table_name}] Inferred schema: {self._schema}")
        return self._schema

    def _apply_lookback_window(self, value: t.Any) -> t.Any:
        """Shift a replication-key starting value back by the configured lookback window.

        DynamoDB scans default to eventually consistent reads (see DynamoDbConnector),
        so a record that was mid-write when a previous scan passed over it may have been
        read stale and silently excluded from the results. Since the SDK's bookmark only
        ever moves forward, that record would otherwise never be re-scanned. Re-including
        a trailing window on every run costs little extra (the destination merge is an
        upsert on primary key) and closes that gap.
        """
        lookback_days = self.config.get("replication_key_lookback_days", 7)
        if not lookback_days:
            return value

        shifted = _shift_lookback(value, lookback_days)
        if shifted is None:
            user_logger.warning(
                f"[{self._table_name}] Replication key value {value!r} isn't a recognized "
                "ISO 8601 datetime string or numeric epoch; skipping lookback window."
            )
            return value
        return shifted

    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Generate records from the stream."""
        total_records = 0
        if self._replication_key and self.get_starting_replication_key_value(context):
            starting_value = self._apply_lookback_window(self.get_starting_replication_key_value(context))
            user_logger.info(
                f"[{self._table_name}] Using replication key: {self.replication_key} with starting value: "
                f"{starting_value} (lookback: {self.config.get('replication_key_lookback_days', 7)} days)"
            )
            self._table_scan_kwargs["FilterExpression"] = "#incremental_filter > :incremental_value"
            self._table_scan_kwargs["ExpressionAttributeNames"] = {"#incremental_filter": self.replication_key}
            self._table_scan_kwargs["ExpressionAttributeValues"] = {":incremental_value": starting_value}

        log_interval = 1000
        next_log_at = log_interval
        try:
            for batch in self._dynamodb_conn.get_items_iter(
                self._table_name,
                self._table_scan_kwargs,
            ):
                total_records += len(batch)
                if total_records >= next_log_at:
                    user_logger.info(f"[{self._table_name}] {total_records} records processed so far...")
                    next_log_at = total_records + log_interval
                for record in batch:
                    try:
                        yield self.process_record(record)
                    except Exception as e:
                        user_logger.error(f"Error processing individual record: {record}. Error details: {str(e)}")
                        sys.exit(1)
            user_logger.info(f"[{self._table_name}] Extraction finished. Total records processed: {total_records}")
        except Exception as e:
            user_logger.error(f"Error getting records for table {self._table_name}. Error details: {str(e)}")
            user_logger.error(f"Table scan kwargs: {self._table_scan_kwargs}")
            sys.exit(1)

    def process_record(self, record: dict) -> dict:
        if self.config.get("extraction_mode") == "envelope":
            processed_record = {
                "_hash_id": self.generate_hash(
                    [record.get(key) for key in self.dynamodb_primary_keys if record.get(key) is not None]
                ),
                "document": json.dumps(record, default=_serialize_dynamodb_value),
            }

            if self.replication_key:
                processed_record[self.replication_key] = record.get(self.replication_key)
        else:
            processed_record = record

        return processed_record

    def generate_hash(self, primary_keys: list[str]) -> str:
        combined_string = "".join(map(str, primary_keys))
        hash_object = hashlib.md5(combined_string.encode("utf-8"))
        return hash_object.hexdigest()
