from tap_dynamodb.streams import _shift_lookback


def test_shift_lookback_iso_string_with_z_suffix():
    assert _shift_lookback("2026-07-29T00:00:00Z", 7) == "2026-07-22T00:00:00Z"


def test_shift_lookback_iso_string_with_fractional_seconds():
    assert _shift_lookback("2026-07-29T00:00:00.123456Z", 7) == "2026-07-22T00:00:00.123456Z"


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
