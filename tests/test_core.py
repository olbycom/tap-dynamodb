"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import json
import os

from moto import mock_dynamodb
from singer_sdk.testing import get_tap_test_class

from tap_dynamodb.tap import TapDynamoDB

SAMPLE_CONFIG = {
    # "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    # TODO: Initialize minimal tap config
}

SAMPLE_CONFIG = json.load(open(".secrets/config.json"))


# Run standard built-in tap tests from the SDK:
# TODO: this doesnt work yet
# with mock_dynamodb():
TestTapDynamoDB = get_tap_test_class(tap_class=TapDynamoDB, config=SAMPLE_CONFIG)
