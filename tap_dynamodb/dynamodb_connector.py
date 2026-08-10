"""DynamoDB connector class."""

import decimal
import sys
from decimal import Clamped, Context, Decimal, Inexact, Overflow, Rounded, Underflow

# Monkey patch boto3's decimal handling
import boto3.dynamodb.types
import genson
import orjson
from botocore.exceptions import ClientError
from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger, user_logger

from tap_dynamodb.connectors.aws_boto_connector import AWSBotoConnector
from tap_dynamodb.schema_helper import cleanup_schema

## Monkey Patch

SAFE_CONTEXT = Context(prec=38, traps=[])  # ← No traps


def safe_create_decimal(value):
    return SAFE_CONTEXT.create_decimal(str(value))


boto3.dynamodb.types.DYNAMODB_CONTEXT = SAFE_CONTEXT
boto3.dynamodb.types.TypeDeserializer.create_decimal = staticmethod(safe_create_decimal)


## -- end of Monkey Patch


class DynamoDbConnector(AWSBotoConnector[DynamoDBServiceResource, DynamoDBClient]):
    """DynamoDB connector class."""

    dynamodb_table_primary_keys = []

    def __init__(
        self,
        config: dict,
    ) -> None:
        """Initialize the connector.

        Args:
            config: The connector configuration.
        """
        super().__init__(config, "dynamodb")

    @staticmethod
    def _coerce_types(record):
        try:

            def handle_unusual_types(obj):
                try:
                    if isinstance(obj, decimal.Decimal):
                        return float(obj)
                    elif isinstance(obj, (set, frozenset)):
                        return list(obj)
                    elif isinstance(obj, bytes):  # for binary data
                        return obj.decode("utf-8", errors="replace")
                    return str(obj)
                except Exception as e:
                    user_logger.error(
                        f"Error in handle_unusual_types function for value {obj} of type {type(obj)}: {str(e)}"
                    )
                    sys.exit(1)

            result = orjson.loads(
                orjson.dumps(
                    record,
                    default=handle_unusual_types,
                    option=orjson.OPT_OMIT_MICROSECONDS,
                ).decode("utf-8")
            )
            return result
        except Exception as e:
            user_logger.error(f"Error processing record: {record} with error: {str(e)}")
            sys.exit(1)

    def list_tables(self, include=None):
        """List tables in DynamoDB."""
        try:
            tables = []
            for table in self.resource.tables.all():
                if include is None or table.name in include:
                    tables.append(table.name)
        except ClientError as err:
            user_logger.error(
                f"Couldn't list tables. Here's why: {err.response['Error']['Code']}: {err.response['Error']['Message']}"
            )
            sys.exit(1)
        else:
            return tables

    def get_items_iter(self, table_name: str, scan_kwargs_override: dict):
        """Get items from a table in DynamoDB via Scan."""
        scan_kwargs = scan_kwargs_override.copy()
        if "ConsistentRead" not in scan_kwargs:
            # Strongly consistent reads cost 2x the RCUs of eventually consistent ones.
            # Full-table streams re-scan everything every run (no bookmark), and
            # incremental streams protect against eventual-consistency staleness with
            # a lookback window (see TableStream._apply_lookback_window), so eventual
            # consistency is safe to default to here. Callers can still force strong
            # consistency per-table via `table_scan_kwargs`.
            scan_kwargs["ConsistentRead"] = False

        table = self.resource.Table(table_name)
        yield from self._paginate_items(table_name, "scan", table.scan, scan_kwargs)

    def get_query_items_iter(self, table_name: str, query_kwargs: dict):
        """Get items from a table in DynamoDB via Query (e.g. against a GSI).

        No ConsistentRead default is applied here -- GSI queries can't be strongly
        consistent at all, and a base-table Query already defaults to eventually
        consistent when the parameter is omitted, so there's nothing to set.
        """
        table = self.resource.Table(table_name)
        yield from self._paginate_items(table_name, "query", table.query, query_kwargs)

    def _paginate_items(self, table_name: str, operation_name: str, operation, kwargs_override: dict):
        """Shared ExclusiveStartKey/LastEvaluatedKey pagination for Scan and Query.

        `operation` is a bound `table.scan` or `table.query` method.
        """
        kwargs = kwargs_override.copy()
        done = False
        start_key = None
        while not done:
            if start_key:
                kwargs["ExclusiveStartKey"] = start_key

            internal_logger.info(f"[{table_name}] Executing {operation_name} with parameters: {kwargs}")

            try:
                response = operation(**kwargs)
            except ClientError as err:
                user_logger.error(
                    f"[{table_name}] Couldn't {operation_name} {table_name}. AWS Error: "
                    f"{err.response['Error']['Code']}: {err.response['Error']['Message']}"
                )
                sys.exit(1)
            except Exception as e:
                user_logger.error(f"[{table_name}] Unexpected error during {operation_name} of {table_name}: {str(e)}")
                sys.exit(1)

            items = response.get("Items", [])

            try:
                processed_items = [self._coerce_types(record) for record in items]
                yield processed_items
            except Exception as e:
                user_logger.error(f"[{table_name}] Error processing items batch from {table_name}: {e}")
                user_logger.error(f"[{table_name}] First few raw items: {items[:2]}")
                sys.exit(1)

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def _get_sample_records(self, table_name: str, sample_size: int, scan_kwargs_override: dict) -> list:
        scan_kwargs = scan_kwargs_override.copy()
        sample_records = []
        if "ConsistentRead" not in scan_kwargs:
            scan_kwargs["ConsistentRead"] = False
        if "Limit" not in scan_kwargs:
            scan_kwargs["Limit"] = sample_size

        for batch in self.get_items_iter(table_name, scan_kwargs):
            sample_records.extend(batch)
            if len(sample_records) >= sample_size:
                break
        return sample_records

    def get_table_json_schema(
        self,
        table_name: str,
        sample_size,
        scan_kwargs: dict,
        key_schema: list | None = None,
        partition_key_values: set | None = None,
        strategy: str = "infer",
    ) -> dict:
        """Get the JSON schema for a table in DynamoDB."""
        sample_records = self._get_sample_records(table_name, sample_size, scan_kwargs)

        if key_schema and partition_key_values:
            self._check_partition_key_coverage(table_name, sample_records, key_schema, partition_key_values)

        if not sample_records:
            user_logger.warning(f"[{table_name}] No records found, generating empty schema.")
            self._primary_keys = self.get_table_key_properties(table_name)
            properties = [th.Property(key, th.StringType) for key in self._primary_keys]
            return th.PropertiesList(*properties).to_dict()

        builder = genson.SchemaBuilder(schema_uri=None)
        for record in sample_records:
            builder.add_object(self._coerce_types(record))
        schema = builder.to_schema()

        final_schema = cleanup_schema(schema)

        if not final_schema:
            user_logger.error(f"[{table_name}] Inferring schema failed.")
            sys.exit(1)
        else:
            user_logger.info(f"[{table_name}] Inferring schema successful for table: '{table_name}'.")
            internal_logger.info(f"[{table_name}] Schema: {final_schema}")

        return final_schema

    def _check_partition_key_coverage(
        self, table_name: str, sample_records: list, key_schema: list, configured_values: set
    ) -> None:
        """Warn if sampled records contain partition key values missing from `table_partition_key_values`.

        `table_partition_key_values` is a manually maintained list (DynamoDB has no way to
        enumerate an attribute's distinct values), so it can silently drift out of sync with
        the real data -- e.g. a new shard added on the write side. This checks the same sample
        already pulled for schema inference against that list on every run, as a cheap tripwire
        for that drift.
        """
        partition_key = next((k["AttributeName"] for k in key_schema if k.get("KeyType") == "HASH"), None)
        if not partition_key:
            return

        seen_values = {record[partition_key] for record in sample_records if partition_key in record}
        unconfigured = seen_values - configured_values

        if unconfigured:
            user_logger.warning(
                f"[{table_name}] Sampled records contain '{partition_key}' values not present in "
                f"table_partition_key_values: {sorted(unconfigured)}. If this table uses "
                "scatter-gather Query extraction, it may be silently missing records outside the "
                "configured partition key values."
            )

    def find_query_index(self, table_name: str, replication_key: str) -> dict | None:
        """Find a GSI whose sort key matches `replication_key`, for scatter-gather Query extraction.

        Returns the GSI's {"IndexName", "KeySchema"} when exactly one candidate exists. Returns
        None -- meaning the caller should fall back to Scan -- when DescribeTable access isn't
        available, no GSI matches, or more than one GSI matches (ambiguous; can't safely pick
        one automatically).
        """
        try:
            table = self.client.describe_table(TableName=table_name)["Table"]
        except ClientError as err:
            user_logger.warning(
                f"[{table_name}] Couldn't call DescribeTable ({err.response['Error']['Code']}): "
                f"{err.response['Error']['Message']}. Skipping GSI auto-discovery for incremental "
                "extraction and falling back to Scan. Grant dynamodb:DescribeTable on this table "
                "to enable cheaper Query-based incremental extraction."
            )
            return None

        matches = [
            gsi
            for gsi in table.get("GlobalSecondaryIndexes", [])
            if any(
                k["AttributeName"] == replication_key and k["KeyType"] == "RANGE"
                for k in gsi.get("KeySchema", [])
            )
        ]

        if not matches:
            internal_logger.info(
                f"[{table_name}] No GSI has '{replication_key}' as its sort key; incremental "
                "extraction will use Scan."
            )
            return None

        if len(matches) > 1:
            names = ", ".join(gsi["IndexName"] for gsi in matches)
            user_logger.warning(
                f"[{table_name}] Multiple GSIs have '{replication_key}' as their sort key ({names}) -- "
                "can't automatically pick one for incremental extraction. Falling back to Scan."
            )
            return None

        gsi = matches[0]

        # A GSI only carries the attributes it projects. Querying one that projects less than ALL
        # would yield records missing most of their fields -- and since the destination upserts on
        # the primary key, those gaps would overwrite existing populated columns with nulls. Falling
        # back to Scan is slower but can't corrupt the table.
        projection_type = gsi.get("Projection", {}).get("ProjectionType")
        if projection_type != "ALL":
            user_logger.warning(
                f"[{table_name}] GSI '{gsi['IndexName']}' matches replication key '{replication_key}', "
                f"but its projection is {projection_type}, not ALL, so a Query against it would return "
                "incomplete records. Falling back to Scan to avoid overwriting existing columns with "
                "nulls on merge."
            )
            return None

        user_logger.info(
            f"[{table_name}] Found GSI '{gsi['IndexName']}' matching replication key '{replication_key}' "
            "(projection: ALL). Configure table_partition_key_values for this table to enable "
            "Query-based incremental extraction instead of Scan."
        )
        return {"IndexName": gsi["IndexName"], "KeySchema": gsi["KeySchema"]}

    def get_table_key_properties(self, table_name):
        """Get the key properties for a table in DynamoDB."""
        key_schema = self.resource.Table(table_name).key_schema
        self.dynamodb_table_primary_keys = [key.get("AttributeName") for key in key_schema]
        return self.dynamodb_table_primary_keys
