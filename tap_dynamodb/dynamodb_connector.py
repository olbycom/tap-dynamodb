import genson
import orjson
from botocore.exceptions import ClientError

from tap_dynamodb.connectors.aws_boto_connector import AWSBotoConnector
from tap_dynamodb.exception import EmptyTableException


class DynamoDbConnector(AWSBotoConnector):
    """DynamoDB connector class."""

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
        return orjson.loads(
            orjson.dumps(
                record,
                default=lambda o: str(o),
                option=orjson.OPT_OMIT_MICROSECONDS,
            ).decode("utf-8")
        )

    def _recursively_drop_required(self, schema: dict) -> None:
        """Recursively drop the required property from a schema.

        This is used to clean up genson generated schemas which are strict by default.

        Args:
            schema: The json schema.
        """
        schema.pop("required", None)
        if "properties" in schema:
            for prop in schema["properties"]:
                if schema["properties"][prop].get("type") == "object":
                    self._recursively_drop_required(schema["properties"][prop])

    def list_tables(self, include=None):
        try:
            tables = []
            for table in self.resource.tables.all():
                if include is None or table.name in include:
                    tables.append(table.name)
        except ClientError as err:
            self.logger.error(
                "Couldn't list tables. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        else:
            return tables

    def get_items_iter(self, table_name: str, scan_kwargs_override: dict):
        scan_kwargs = scan_kwargs_override.copy()
        if "ConsistentRead" not in scan_kwargs:
            scan_kwargs["ConsistentRead"] = True

        table = self.resource.Table(table_name)
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = table.scan(**scan_kwargs)
                yield [
                    self._coerce_types(record) for record in response.get("Items", [])
                ]
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except ClientError as err:
            self.logger.error(
                "Couldn't scan for %s. Here's why: %s: %s",
                table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def _get_sample_records(
        self, table_name: str, sample_size: int, scan_kwargs_override: dict
    ) -> list:
        scan_kwargs = scan_kwargs_override.copy()
        sample_records = []
        if "ConsistentRead" not in scan_kwargs:
            scan_kwargs["ConsistentRead"] = True
        if "Limit" not in scan_kwargs:
            scan_kwargs["Limit"] = sample_size

        for batch in self.get_items_iter(table_name, scan_kwargs):
            sample_records.extend(batch)
            if len(sample_records) >= sample_size:
                break
        return sample_records

    def get_table_json_schema(
        self, table_name: str, sample_size, scan_kwargs: dict, strategy: str = "infer"
    ) -> dict:
        sample_records = self._get_sample_records(table_name, sample_size, scan_kwargs)

        if not sample_records:
            raise EmptyTableException()
        if strategy == "infer":
            builder = genson.SchemaBuilder(schema_uri=None)
            for record in sample_records:
                builder.add_object(self._coerce_types(record))
            schema = builder.to_schema()
            self._recursively_drop_required(schema)
            if not schema:
                raise Exception("Inferring schema failed")
            else:
                self.logger.info(
                    f"Inferring schema successful for table: '{table_name}'"
                )
        else:
            raise Exception(f"Strategy {strategy} not supported")
        return schema

    def get_table_key_properties(self, table_name):
        key_schema = self.resource.Table(table_name).key_schema
        return [key.get("AttributeName") for key in key_schema]
