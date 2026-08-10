from tap_dynamodb.streams import TableStream, _parse_partition_key_values, _shift_lookback


def test_shift_lookback_iso_string_with_z_suffix():
    assert _shift_lookback("2026-07-29T00:00:00Z", 7) == "2026-07-22T00:00:00Z"


def test_shift_lookback_iso_string_with_fractional_seconds():
    assert _shift_lookback("2026-07-29T00:00:00.123456Z", 7) == "2026-07-22T00:00:00.123456Z"


def test_shift_lookback_space_separated_datetime():
    # The format DatahubProcessos_PROD actually stores (space separator, no timezone).
    assert _shift_lookback("2026-08-03 23:50:02", 7) == "2026-07-27 23:50:02"


def test_shift_lookback_space_separated_preserves_fractional_seconds():
    assert _shift_lookback("2026-08-03 23:50:02.123", 7) == "2026-07-27 23:50:02.123"


def test_shift_lookback_date_only():
    assert _shift_lookback("2026-08-03", 7) == "2026-07-27"


def test_shift_lookback_iso_string_with_offset():
    assert _shift_lookback("2026-07-29T00:00:00+00:00", 1) == "2026-07-28T00:00:00+00:00"


def test_shift_lookback_iso_string_short_fraction_preserves_digit_count():
    assert _shift_lookback("2026-07-29T00:00:00.5Z", 1) == "2026-07-28T00:00:00.5Z"


def test_shift_lookback_epoch_seconds():
    assert _shift_lookback(1_753_747_200, 1) == 1_753_747_200 - 86400


def test_shift_lookback_epoch_millis():
    assert _shift_lookback(1_753_747_200_000, 1) == 1_753_747_200_000 - 86_400_000


def test_shift_lookback_unsupported_type_returns_none():
    assert _shift_lookback(["not", "supported"], 7) is None


def test_shift_lookback_unrecognized_string_returns_none():
    assert _shift_lookback("not-a-date", 7) is None


def test_parse_partition_key_values_splits_and_strips():
    entries = [{"table_name": "MyTable", "partition_key_values": "SHARD#1, SHARD#2 ,SHARD#3"}]
    assert _parse_partition_key_values(entries, "MyTable") == {"SHARD#1", "SHARD#2", "SHARD#3"}


def test_parse_partition_key_values_returns_none_when_table_not_configured():
    entries = [{"table_name": "OtherTable", "partition_key_values": "SHARD#1"}]
    assert _parse_partition_key_values(entries, "MyTable") is None


def test_parse_partition_key_values_empty_entries_list():
    assert _parse_partition_key_values([], "MyTable") is None


class _FakeConn:
    def __init__(self):
        self.scan_calls = []
        self.query_calls = []

    def get_items_iter(self, table_name, kwargs):
        self.scan_calls.append((table_name, kwargs))
        yield [{"id": "scan-result"}]

    def get_query_items_iter(self, table_name, kwargs):
        self.query_calls.append((table_name, kwargs))
        yield [{"id": f"query-result-{kwargs['ExpressionAttributeValues'][':pk']}"}]


def _make_table_stream(query_index=None, partition_key_values=None, replication_key="UpdatedAt", dynamodb_conn=None):
    """Build a TableStream without running __init__ (which needs a real Tap/catalog)."""
    stream = TableStream.__new__(TableStream)
    stream._table_name = "MyTable"
    stream._query_index = query_index
    stream._partition_key_values = partition_key_values
    stream._table_scan_kwargs = {}
    stream._replication_key = replication_key
    stream._dynamodb_conn = dynamodb_conn or _FakeConn()
    return stream


_GSI = {
    "IndexName": "my-gsi",
    "KeySchema": [
        {"AttributeName": "ShardKey", "KeyType": "HASH"},
        {"AttributeName": "UpdatedAt", "KeyType": "RANGE"},
    ],
}


def test_get_batches_uses_query_when_index_and_values_and_starting_value_present():
    conn = _FakeConn()
    stream = _make_table_stream(query_index=_GSI, partition_key_values={"A", "B"}, dynamodb_conn=conn)

    list(stream._get_batches("2026-01-01T00:00:00Z"))

    assert conn.scan_calls == []
    assert len(conn.query_calls) == 2
    pk_values = {kwargs["ExpressionAttributeValues"][":pk"] for _, kwargs in conn.query_calls}
    assert pk_values == {"A", "B"}
    for table_name, kwargs in conn.query_calls:
        assert table_name == "MyTable"
        assert kwargs["IndexName"] == "my-gsi"
        assert kwargs["KeyConditionExpression"] == "#pk = :pk AND #rk > :cutoff"
        assert kwargs["ExpressionAttributeNames"] == {"#pk": "ShardKey", "#rk": "UpdatedAt"}
        assert kwargs["ExpressionAttributeValues"][":cutoff"] == "2026-01-01T00:00:00Z"


def test_get_batches_falls_back_to_scan_without_starting_value():
    conn = _FakeConn()
    stream = _make_table_stream(query_index=_GSI, partition_key_values={"A"}, dynamodb_conn=conn)

    list(stream._get_batches(None))

    assert conn.query_calls == []
    assert len(conn.scan_calls) == 1
    assert "FilterExpression" not in stream._table_scan_kwargs


def test_get_batches_falls_back_to_scan_without_partition_key_values():
    conn = _FakeConn()
    stream = _make_table_stream(query_index=_GSI, partition_key_values=None, dynamodb_conn=conn)

    list(stream._get_batches("2026-01-01T00:00:00Z"))

    assert conn.query_calls == []
    assert len(conn.scan_calls) == 1
    assert stream._table_scan_kwargs["FilterExpression"] == "#incremental_filter > :incremental_value"
    assert stream._table_scan_kwargs["ExpressionAttributeValues"] == {":incremental_value": "2026-01-01T00:00:00Z"}


def test_get_batches_falls_back_to_scan_without_query_index():
    conn = _FakeConn()
    stream = _make_table_stream(query_index=None, partition_key_values={"A"}, dynamodb_conn=conn)

    list(stream._get_batches("2026-01-01T00:00:00Z"))

    assert conn.query_calls == []
    assert len(conn.scan_calls) == 1


def test_get_query_batches_warns_and_falls_back_when_gsi_has_no_partition_key(monkeypatch):
    conn = _FakeConn()
    broken_gsi = {"IndexName": "broken-gsi", "KeySchema": [{"AttributeName": "UpdatedAt", "KeyType": "RANGE"}]}
    stream = _make_table_stream(query_index=broken_gsi, partition_key_values={"A"}, dynamodb_conn=conn)

    warnings = []
    monkeypatch.setattr("tap_dynamodb.streams.user_logger.warning", lambda msg: warnings.append(msg))

    list(stream._get_batches("2026-01-01T00:00:00Z"))

    assert len(warnings) == 1
    assert "broken-gsi" in warnings[0]
    assert conn.query_calls == []
    assert len(conn.scan_calls) == 1
