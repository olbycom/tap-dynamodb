"""Instagram entry point."""

from __future__ import annotations

from tap_dynamodb.tap import TapDynamoDB

TapDynamoDB.cli()
