import genson
import orjson
from botocore.exceptions import ClientError

from tap_dynamodb.aws_authenticators import AWSBotoAuthenticator
from tap_dynamodb.exception import EmptyTableException


class DynamoDB(AWSBotoAuthenticator):
    def __init__(self, config):
        super().__init__(config, "dynamodb")

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

    @staticmethod
    def _coerce_types(record):
        return orjson.loads(
            orjson.dumps(
                record,
                default=lambda o: str(o),
                option=orjson.OPT_OMIT_MICROSECONDS,
            ).decode("utf-8")
        )

    def get_items_iter(
        self, table_name: str, scan_kwargs: dict = {"ConsistentRead": True}
    ):
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
                "Couldn't scan for movies. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise

    def get_table_json_schema(self, table_name: str, strategy: str = "infer"):
        sample_records = list(
            self.get_items_iter(
                table_name, scan_kwargs={"Limit": 100, "ConsistentRead": True}
            )
        )[0]
        if not sample_records:
            raise EmptyTableException()
        if strategy == "infer":
            builder = genson.SchemaBuilder(schema_uri=None)
            for record in sample_records:
                builder.add_object(self._coerce_types(record))
            schema = builder.to_schema()
            self.recursively_drop_required(schema)
            if not schema:
                raise Exception("Inferring schema failed")
            else:
                self.logger.info(
                    f"Inferring schema successful for table: '{table_name}'"
                )
        else:
            raise Exception(f"Strategy {strategy} not supported")
        return schema

    def recursively_drop_required(self, schema: dict) -> None:
        """Recursively drop the required property from a schema.

        This is used to clean up genson generated schemas which are strict by default.

        Args:
            schema: The json schema.
        """
        schema.pop("required", None)
        if "properties" in schema:
            for prop in schema["properties"]:
                if schema["properties"][prop].get("type") == "object":
                    self.recursively_drop_required(schema["properties"][prop])
