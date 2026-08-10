import boto3
from moto import mock_aws

from tap_dynamodb.dynamodb_connector import DynamoDbConnector

SAMPLE_CONFIG = {
    "aws_access_key_id": "foo",
    "aws_secret_access_key": "bar",
    "aws_default_region": "us-west-2",
}


def create_table(moto_conn, name):
    return moto_conn.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "year", "KeyType": "HASH"},
            {"AttributeName": "title", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "year", "AttributeType": "N"},
            {"AttributeName": "title", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
    )


@mock_aws
def test_list_tables():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    for num in range(1, 106):
        create_table(moto_conn, f"table_{num}")
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    tables = db_obj.list_tables()
    assert len(tables) == 105
    assert tables[0] == "table_1"
    assert tables[-1] == "table_105"


@mock_aws
def test_list_tables_filtered():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    create_table(moto_conn, "table_to_replicate")
    create_table(moto_conn, "table_to_skip")
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    tables = db_obj.list_tables(["table_to_replicate"])
    assert len(tables) == 1
    assert tables[0] == "table_to_replicate"
    tables = db_obj.list_tables()
    assert len(tables) == 2
    assert tables == ["table_to_replicate", "table_to_skip"]
    tables = db_obj.list_tables([])
    assert len(tables) == 0


@mock_aws
def test_get_items():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    table.put_item(Item={"year": 2023, "title": "foo", "info": {"plot": "bar"}})
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    records = list(db_obj.get_items_iter("table", {}))[0]
    assert len(records) == 1
    # Type coercion
    assert records[0].get("year") == "2023"
    assert records[0].get("title") == "foo"
    assert records[0].get("info") == {"plot": "bar"}


@mock_aws
def test_get_items_w_kwargs():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    table.put_item(Item={"year": 2023, "title": "foo", "info": {"plot": "bar"}})
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    records = list(
        db_obj.get_items_iter(
            "table",
            {"Select": "SPECIFIC_ATTRIBUTES", "ProjectionExpression": "title, info"},
        )
    )[0]
    assert len(records) == 1
    # Type coercion
    assert records[0].get("title") == "foo"
    assert records[0].get("info") == {"plot": "bar"}


@mock_aws
def test_get_items_paginate():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    for num in range(5):
        table.put_item(
            Item={"year": 2023, "title": f"foo_{num}", "info": {"plot": "bar"}}
        )
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    iterations = 0
    records = []
    for i in db_obj.get_items_iter("table", {"Limit": 1, "ConsistentRead": True}):
        iterations += 1
        records.extend(i)
    assert len(records) == 5
    assert iterations == 5
    first_item = records[0]
    assert first_item.get("year") == "2023"
    assert first_item.get("title") == "foo_0"
    assert first_item.get("info") == {"plot": "bar"}


@mock_aws
def test_get_table_json_schema():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    for num in range(5):
        table.put_item(
            Item={"year": 2023, "title": f"foo_{num}", "info": {"plot": "bar"}}
        )
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    schema = db_obj.get_table_json_schema("table", 5, {})
    assert schema == {
        "type": "object",
        "properties": {
            "year": {"type": "string"},
            "title": {"type": "string"},
            "info": {"type": "object", "properties": {"plot": {"type": "string"}}},
        },
    }


@mock_aws
def test_get_table_json_schema_w_kwargs():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    for num in range(5):
        table.put_item(
            Item={"year": 2023, "title": f"foo_{num}", "info": {"plot": "bar"}}
        )
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    schema = db_obj.get_table_json_schema(
        "table",
        5,
        {"Select": "SPECIFIC_ATTRIBUTES", "ProjectionExpression": "title, info"},
    )
    assert schema == {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "info": {"type": "object", "properties": {"plot": {"type": "string"}}},
        },
    }


@mock_aws
def test_get_table_key_properties():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    for num in range(5):
        table.put_item(
            Item={"year": 2023, "title": f"foo_{num}", "info": {"plot": "bar"}}
        )
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    assert ["year", "title"] == db_obj.get_table_key_properties("table")


def test_coerce_types():
    import decimal

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    coerced = db_obj._coerce_types({"foo": decimal.Decimal("1.23")})
    assert coerced == {"foo": "1.23"}


@mock_aws
def test_get_sample_records():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    for num in range(5):
        table.put_item(
            Item={"year": 2023, "title": f"foo_{num}", "info": {"plot": "bar"}}
        )
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    records = db_obj._get_sample_records("table", 2, {})
    assert len(records) == 2


@mock_aws
def test_get_table_json_schema_warns_on_unconfigured_partition_key_value(monkeypatch):
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    for num in range(5):
        table.put_item(Item={"year": 2023, "title": f"foo_{num}", "info": {"plot": "bar"}})
    # END PREP

    warnings = []
    monkeypatch.setattr(
        "tap_dynamodb.dynamodb_connector.user_logger.warning",
        lambda msg: warnings.append(msg),
    )

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    db_obj.get_table_json_schema(
        "table",
        5,
        {},
        key_schema=[{"AttributeName": "title", "KeyType": "HASH"}],
        partition_key_values={"foo_0", "foo_1"},
    )

    assert len(warnings) == 1
    assert "title" in warnings[0]
    assert "foo_2" in warnings[0]
    assert "foo_3" in warnings[0]
    assert "foo_4" in warnings[0]


@mock_aws
def test_get_table_json_schema_no_warning_when_fully_covered(monkeypatch):
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    for num in range(5):
        table.put_item(Item={"year": 2023, "title": f"foo_{num}", "info": {"plot": "bar"}})
    # END PREP

    warnings = []
    monkeypatch.setattr(
        "tap_dynamodb.dynamodb_connector.user_logger.warning",
        lambda msg: warnings.append(msg),
    )

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    db_obj.get_table_json_schema(
        "table",
        5,
        {},
        key_schema=[{"AttributeName": "title", "KeyType": "HASH"}],
        partition_key_values={f"foo_{n}" for n in range(5)},
    )

    assert warnings == []


@mock_aws
def test_get_table_json_schema_skips_check_without_query_index(monkeypatch):
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table(moto_conn, "table")
    table.put_item(Item={"year": 2023, "title": "foo_0", "info": {"plot": "bar"}})
    # END PREP

    warnings = []
    monkeypatch.setattr(
        "tap_dynamodb.dynamodb_connector.user_logger.warning",
        lambda msg: warnings.append(msg),
    )

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    db_obj.get_table_json_schema("table", 5, {})

    assert warnings == []


def create_table_with_gsi(moto_conn, name, gsi_name, gsi_sort_key, projection=None):
    return moto_conn.create_table(
        TableName=name,
        KeySchema=[
            {"AttributeName": "year", "KeyType": "HASH"},
            {"AttributeName": "title", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "year", "AttributeType": "N"},
            {"AttributeName": "title", "AttributeType": "S"},
            {"AttributeName": "ShardKey", "AttributeType": "S"},
            {"AttributeName": gsi_sort_key, "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        GlobalSecondaryIndexes=[
            {
                "IndexName": gsi_name,
                "KeySchema": [
                    {"AttributeName": "ShardKey", "KeyType": "HASH"},
                    {"AttributeName": gsi_sort_key, "KeyType": "RANGE"},
                ],
                "Projection": projection or {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
            }
        ],
    )


@mock_aws
def test_get_query_items_iter_filters_by_partition_and_sort_key():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table_with_gsi(moto_conn, "table", "my-gsi", "UpdatedAt")
    table.put_item(Item={"year": 2023, "title": "a", "ShardKey": "SHARD#1", "UpdatedAt": "2026-01-01T00:00:00Z"})
    table.put_item(Item={"year": 2023, "title": "b", "ShardKey": "SHARD#1", "UpdatedAt": "2026-01-03T00:00:00Z"})
    table.put_item(Item={"year": 2023, "title": "c", "ShardKey": "SHARD#2", "UpdatedAt": "2026-01-05T00:00:00Z"})
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    batches = list(
        db_obj.get_query_items_iter(
            "table",
            {
                "IndexName": "my-gsi",
                "KeyConditionExpression": "#pk = :pk AND #rk > :cutoff",
                "ExpressionAttributeNames": {"#pk": "ShardKey", "#rk": "UpdatedAt"},
                "ExpressionAttributeValues": {":pk": "SHARD#1", ":cutoff": "2026-01-02T00:00:00Z"},
            },
        )
    )
    titles = {record["title"] for batch in batches for record in batch}

    # Only "b" matches: same shard as "a"/"b", but after the cutoff (excludes "a"); "c" is a different shard.
    assert titles == {"b"}


@mock_aws
def test_get_query_items_iter_paginates():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    table = create_table_with_gsi(moto_conn, "table", "my-gsi", "UpdatedAt")
    for num in range(5):
        table.put_item(
            Item={"year": 2023, "title": f"foo_{num}", "ShardKey": "SHARD#1", "UpdatedAt": f"2026-01-0{num + 1}T00:00:00Z"}
        )
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    iterations = 0
    records = []
    for batch in db_obj.get_query_items_iter(
        "table",
        {
            "IndexName": "my-gsi",
            "Limit": 1,
            "KeyConditionExpression": "#pk = :pk",
            "ExpressionAttributeNames": {"#pk": "ShardKey"},
            "ExpressionAttributeValues": {":pk": "SHARD#1"},
        },
    ):
        iterations += 1
        records.extend(batch)

    assert iterations == 5
    assert len(records) == 5


@mock_aws
def test_find_query_index_returns_matching_gsi():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    create_table_with_gsi(moto_conn, "table", "my-gsi", "UpdatedAt")
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    result = db_obj.find_query_index("table", "UpdatedAt")

    assert result["IndexName"] == "my-gsi"
    assert {"AttributeName": "ShardKey", "KeyType": "HASH"} in result["KeySchema"]
    assert {"AttributeName": "UpdatedAt", "KeyType": "RANGE"} in result["KeySchema"]


@mock_aws
def test_find_query_index_warns_and_returns_none_on_keys_only_projection(monkeypatch):
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    create_table_with_gsi(moto_conn, "table", "my-gsi", "UpdatedAt", projection={"ProjectionType": "KEYS_ONLY"})
    # END PREP

    warnings = []
    monkeypatch.setattr(
        "tap_dynamodb.dynamodb_connector.user_logger.warning",
        lambda msg: warnings.append(msg),
    )

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    result = db_obj.find_query_index("table", "UpdatedAt")

    assert result is None
    assert len(warnings) == 1
    assert "KEYS_ONLY" in warnings[0]
    assert "my-gsi" in warnings[0]


@mock_aws
def test_find_query_index_warns_and_returns_none_on_include_projection(monkeypatch):
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    create_table_with_gsi(
        moto_conn,
        "table",
        "my-gsi",
        "UpdatedAt",
        projection={"ProjectionType": "INCLUDE", "NonKeyAttributes": ["info"]},
    )
    # END PREP

    warnings = []
    monkeypatch.setattr(
        "tap_dynamodb.dynamodb_connector.user_logger.warning",
        lambda msg: warnings.append(msg),
    )

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    result = db_obj.find_query_index("table", "UpdatedAt")

    assert result is None
    assert len(warnings) == 1
    assert "INCLUDE" in warnings[0]


@mock_aws
def test_find_query_index_returns_none_when_no_match():
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    create_table_with_gsi(moto_conn, "table", "my-gsi", "UpdatedAt")
    # END PREP

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    assert db_obj.find_query_index("table", "SomeOtherField") is None


@mock_aws
def test_find_query_index_warns_and_returns_none_on_multiple_matches(monkeypatch):
    # PREP
    moto_conn = boto3.resource("dynamodb", region_name="us-west-2")
    moto_conn.create_table(
        TableName="table",
        KeySchema=[{"AttributeName": "year", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "year", "AttributeType": "N"},
            {"AttributeName": "ShardKeyA", "AttributeType": "S"},
            {"AttributeName": "ShardKeyB", "AttributeType": "S"},
            {"AttributeName": "UpdatedAt", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi-a",
                "KeySchema": [
                    {"AttributeName": "ShardKeyA", "KeyType": "HASH"},
                    {"AttributeName": "UpdatedAt", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
            },
            {
                "IndexName": "gsi-b",
                "KeySchema": [
                    {"AttributeName": "ShardKeyB", "KeyType": "HASH"},
                    {"AttributeName": "UpdatedAt", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
            },
        ],
    )
    # END PREP

    warnings = []
    monkeypatch.setattr(
        "tap_dynamodb.dynamodb_connector.user_logger.warning",
        lambda msg: warnings.append(msg),
    )

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    result = db_obj.find_query_index("table", "UpdatedAt")

    assert result is None
    assert len(warnings) == 1
    assert "gsi-a" in warnings[0]
    assert "gsi-b" in warnings[0]


def test_find_query_index_warns_and_returns_none_without_describe_table_access(monkeypatch):
    from botocore.exceptions import ClientError

    warnings = []
    monkeypatch.setattr(
        "tap_dynamodb.dynamodb_connector.user_logger.warning",
        lambda msg: warnings.append(msg),
    )

    class _DeniedClient:
        exceptions = None

        def describe_table(self, TableName):  # noqa: N803 (matches boto3's API)
            raise ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "not authorized"}},
                "DescribeTable",
            )

    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    monkeypatch.setattr(DynamoDbConnector, "client", property(lambda self: _DeniedClient()))

    result = db_obj.find_query_index("table", "UpdatedAt")

    assert result is None
    assert len(warnings) == 1
    assert "AccessDeniedException" in warnings[0]


class _FakeTable:
    def __init__(self):
        self.scan_calls = []

    def scan(self, **kwargs):
        self.scan_calls.append(kwargs)
        return {"Items": []}


class _FakeResource:
    def __init__(self, table):
        self._table = table

    def Table(self, name):  # noqa: N802 (matches boto3's resource API)
        return self._table


def test_get_items_iter_defaults_to_eventually_consistent(monkeypatch):
    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    fake_table = _FakeTable()
    monkeypatch.setattr(DynamoDbConnector, "resource", property(lambda self: _FakeResource(fake_table)))

    list(db_obj.get_items_iter("table", {}))

    assert fake_table.scan_calls[0]["ConsistentRead"] is False


def test_get_items_iter_respects_consistent_read_override(monkeypatch):
    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    fake_table = _FakeTable()
    monkeypatch.setattr(DynamoDbConnector, "resource", property(lambda self: _FakeResource(fake_table)))

    list(db_obj.get_items_iter("table", {"ConsistentRead": True}))

    assert fake_table.scan_calls[0]["ConsistentRead"] is True


def test_get_sample_records_defaults_to_eventually_consistent(monkeypatch):
    db_obj = DynamoDbConnector(SAMPLE_CONFIG)
    fake_table = _FakeTable()
    monkeypatch.setattr(DynamoDbConnector, "resource", property(lambda self: _FakeResource(fake_table)))

    db_obj._get_sample_records("table", 2, {})

    assert fake_table.scan_calls[0]["ConsistentRead"] is False
