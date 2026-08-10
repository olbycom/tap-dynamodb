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

from tap_dynamodb.exception import QueryAccessDeniedException

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from nekt_singer_sdk.helpers.types import Context
    from nekt_singer_sdk.tap_base import Tap

    from tap_dynamodb.dynamodb_connector import DynamoDbConnector


# The separator is captured as part of `rest` (not matched separately), so whichever one the
# source table uses -- "T" per ISO 8601, or the equally common space -- is spliced back verbatim.
_DATE_PREFIX_PATTERN = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})(?P<rest>[T ].*)?$")


def _shift_lookback(value: t.Any, lookback_days: int) -> t.Any | None:
    """Shift a replication-key starting value back by `lookback_days`.

    For strings, only the calendar date is recomputed; the separator and the time-of-day/
    fractional-seconds/timezone substring after it (if any) are spliced back verbatim
    rather than reparsed and reformatted. DynamoDB compares this value against the raw
    stored attribute lexicographically, so it must keep whatever precision/format the
    source table already uses — not reconstructing that substring at all is safer than
    trying to reproduce it. Numeric epoch values (seconds or milliseconds, by magnitude)
    are handled by plain arithmetic. Returns None if `value`'s type or format isn't
    recognized, leaving the decision to skip the lookback to the caller.
    """
    if isinstance(value, str):
        match = _DATE_PREFIX_PATTERN.match(value)
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


def _parse_partition_key_values(entries: list, table_name: str) -> set[str] | None:
    """Find `table_name`'s entry in `table_partition_key_values` and parse its comma-separated values.

    Returns None if the table has no entry configured (distinct from an empty set, which would
    mean an entry exists but its value list is empty).
    """
    for entry in entries:
        if entry.get("table_name") == table_name:
            raw = entry.get("partition_key_values", "")
            return {v.strip() for v in raw.split(",") if v.strip()}
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
        query_index: dict | None = None,
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
            query_index: The GSI {"IndexName", "KeySchema"} auto-discovered for this table's
                replication key, if any (see DynamoDbConnector.find_query_index).
        """
        self.user_defined_replication_key = replication_key
        self.user_defined_replication_method = replication_method
        self.deserializer = TypeDeserializer()

        self._dynamodb_conn: DynamoDbConnector = dynamodb_conn
        self._table_name: str = name
        self._schema: dict = {}
        self._infer_schema_sample_size = infer_schema_sample_size
        self._table_scan_kwargs: dict = tap.config.get("table_scan_kwargs", {}).get(name, {})
        self._query_index = query_index
        self._partition_key_values = _parse_partition_key_values(
            tap.config.get("table_partition_key_values", []), name
        )
        self._extraction_notices: list[str] = []
        self._extraction_mode: str = "Scan"
        self._shard_counts: dict[str, int] = {}
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
                    self._query_index["KeySchema"] if self._query_index else None,
                    self._partition_key_values,
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
                f"[{self._table_name}] Replication key value {value!r} doesn't start with a "
                "YYYY-MM-DD date and isn't a numeric epoch; skipping lookback window. Incremental "
                "runs are unprotected against eventually consistent reads missing recent writes."
            )
            return value
        return shifted

    def _notice(self, message: str, *, level: str = "warning") -> None:
        """Log a notable extraction condition now, and queue it for the end-of-run summary.

        Anything explaining why extraction behaved differently than configured belongs here:
        these lines scroll far out of view during a multi-hour run, and the summary at the end
        is where they actually get read.
        """
        getattr(user_logger, level)(f"[{self._table_name}] {message}")
        self._extraction_notices.append(message)

    def _scan_batches(self, starting_value: t.Any) -> Iterable[list]:
        """Yield item batches via Scan, filtering on the replication key when there's a cutoff."""
        if starting_value is not None:
            self._table_scan_kwargs["FilterExpression"] = "#incremental_filter > :incremental_value"
            self._table_scan_kwargs["ExpressionAttributeNames"] = {"#incremental_filter": self.replication_key}
            self._table_scan_kwargs["ExpressionAttributeValues"] = {":incremental_value": starting_value}

        yield from self._dynamodb_conn.get_items_iter(self._table_name, self._table_scan_kwargs)

    def _get_batches(self, starting_value: t.Any) -> Iterable[list]:
        """Choose scatter-gather Query (if eligible) or Scan, and yield item batches.

        Query is only used once there's an actual starting value to filter on -- the very
        first run for a stream (no bookmark yet) needs every item regardless of partition,
        so it uses Scan same as it always has, exactly like the Scan path does when there's
        no starting value at all.
        """
        if self._query_index and self._partition_key_values and starting_value is not None:
            yield from self._get_query_batches(starting_value)
            return

        if self._query_index and not self._partition_key_values:
            self._notice(
                f"GSI '{self._query_index['IndexName']}' is available for Query-based incremental "
                "extraction, but table_partition_key_values isn't configured for this table -- "
                "using Scan. Configuring it would read only changed records instead of the whole table.",
                level="info",
            )
        elif self._query_index and self._partition_key_values and starting_value is None:
            self._notice(
                "Query scatter-gather is configured but there's no starting replication key value "
                "yet (first run for this stream) -- using Scan for this bootstrap load.",
                level="info",
            )

        self._extraction_mode = "Scan"
        yield from self._scan_batches(starting_value)

    def _get_query_batches(self, starting_value: t.Any) -> Iterable[list]:
        """Scatter-gather Query the discovered GSI once per configured partition key value."""
        partition_key = next(
            (k["AttributeName"] for k in self._query_index["KeySchema"] if k["KeyType"] == "HASH"),
            None,
        )
        if not partition_key:
            self._notice(
                f"Discovered GSI '{self._query_index['IndexName']}' has no partition key in its "
                "KeySchema; falling back to Scan."
            )
            self._extraction_mode = "Scan (fell back: GSI has no partition key)"
            yield from self._scan_batches(starting_value)
            return

        self._extraction_mode = f"Query on GSI '{self._query_index['IndexName']}'"
        user_logger.info(
            f"[{self._table_name}] Using Query against GSI '{self._query_index['IndexName']}' across "
            f"{len(self._partition_key_values)} partition key value(s) instead of Scan."
        )
        shard_counts: dict[str, int] = {}
        emitted = 0
        for shard_value in sorted(self._partition_key_values):
            query_kwargs = {
                "IndexName": self._query_index["IndexName"],
                "KeyConditionExpression": "#pk = :pk AND #rk > :cutoff",
                "ExpressionAttributeNames": {"#pk": partition_key, "#rk": self.replication_key},
                "ExpressionAttributeValues": {":pk": shard_value, ":cutoff": starting_value},
            }
            shard_total = 0
            try:
                for batch in self._dynamodb_conn.get_query_items_iter(self._table_name, query_kwargs):
                    shard_total += len(batch)
                    emitted += len(batch)
                    yield batch
            except QueryAccessDeniedException as err:
                # Querying a GSI needs dynamodb:Query on the index ARN, which is a separate
                # resource from the table ARN -- a common policy gap. Scanning still works, so
                # degrade to it rather than failing the run outright.
                if emitted:
                    # Re-running as a Scan would re-emit what we already yielded. Harmless for an
                    # upsert destination, but ambiguous enough that failing loudly is better.
                    user_logger.error(
                        f"[{self._table_name}] dynamodb:Query was denied partway through extraction "
                        f"(after {emitted} record(s)): {err}"
                    )
                    sys.exit(1)
                self._notice(
                    f"dynamodb:Query denied on GSI '{self._query_index['IndexName']}' -- falling back "
                    "to a full-table Scan, which is much slower and reads the entire table. Grant "
                    "dynamodb:Query on the index ARN (arn:aws:dynamodb:<region>:<account>:table/"
                    f"{self._table_name}/index/*), which is a separate IAM resource from the table "
                    f"ARN. AWS said: {err}"
                )
                self._extraction_mode = "Scan (fell back: dynamodb:Query denied on the index)"
                yield from self._scan_batches(starting_value)
                return

            shard_counts[shard_value] = shard_total
            user_logger.info(f"[{self._table_name}] Partition key '{shard_value}': {shard_total} record(s).")

        self._shard_counts = shard_counts
        user_logger.info(f"[{self._table_name}] Query scatter-gather complete. Per-shard counts: {shard_counts}")

    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Generate records from the stream."""
        total_records = 0
        starting_value = None
        if self._replication_key and self.get_starting_replication_key_value(context):
            starting_value = self._apply_lookback_window(self.get_starting_replication_key_value(context))
            user_logger.info(
                f"[{self._table_name}] Using replication key: {self.replication_key} with starting value: "
                f"{starting_value} (lookback: {self.config.get('replication_key_lookback_days', 7)} days)"
            )

        log_interval = 1000
        next_log_at = log_interval
        try:
            for batch in self._get_batches(starting_value):
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
            self._log_extraction_summary(total_records)
        except Exception as e:
            user_logger.error(f"Error getting records for table {self._table_name}. Error details: {str(e)}")
            user_logger.error(f"Table scan kwargs: {self._table_scan_kwargs}")
            sys.exit(1)

    def _log_extraction_summary(self, total_records: int) -> None:
        """Emit a recap of how extraction actually ran, at the end where users read the logs."""
        lines = [
            f"[{self._table_name}] Extraction summary",
            f"  Mode:    {self._extraction_mode}",
            f"  Records: {total_records:,}",
        ]
        if self._shard_counts:
            counts = ", ".join(f"{shard}={count:,}" for shard, count in sorted(self._shard_counts.items()))
            lines.append(f"  Per partition key: {counts}")

        if self._extraction_notices:
            lines.append(f"  Notices ({len(self._extraction_notices)}):")
            lines.extend(f"    - {notice}" for notice in self._extraction_notices)
            user_logger.warning("\n".join(lines))
        else:
            user_logger.info("\n".join(lines))

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
