import pytest
from contextlib import nullcontext as does_not_raise
from unittest import mock
from moto import mock_dynamodb, mock_sts
from tap_dynamodb.aws_auth import AWSAuth
import os
from unittest.mock import patch
import botocore

@patch("tap_dynamodb.aws_auth.boto3.Session", return_value="mock_session")
@mock_dynamodb
def test_get_session_base(patch):
    auth = AWSAuth(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        }
    )
    session = auth.get_session()
    patch.assert_called_with(
        aws_access_key_id="foo",
        aws_secret_access_key="bar",
        region_name="baz",
    )
    assert session == "mock_session"

@patch("tap_dynamodb.aws_auth.boto3.Session", return_value="mock_session")
@mock_dynamodb
def test_get_session_w_token(patch):
    auth = AWSAuth(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_session_token": "abc",
            "aws_default_region": "baz",
        }
    )
    session = auth.get_session()
    patch.assert_called_with(
        aws_access_key_id="foo",
        aws_secret_access_key="bar",
        aws_session_token="abc",
        region_name="baz",
    )
    assert session == "mock_session"


@patch("tap_dynamodb.aws_auth.boto3.Session", return_value="mock_session")
@mock_dynamodb
def test_get_session_w_profile(patch):
    auth = AWSAuth(
        {
            "aws_profile": "foo",
        }
    )
    session = auth.get_session()
    patch.assert_called_with(
        profile_name="foo",
    )
    assert session == "mock_session"


@mock_dynamodb
def test_get_session_empty_fail():
    with pytest.raises(Exception):
        auth = AWSAuth({})
        auth.get_session()


@mock_dynamodb
@mock_sts
def test_get_session_assume_role():
    auth = AWSAuth(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
            "aws_assume_role_arn": "arn:aws:iam::123456778910:role/my-role-name"
        }
    )
    session = auth.get_session()


@mock_dynamodb
def test_get_client():
    auth = AWSAuth(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        }
    )
    session = auth.get_session()
    client = auth.get_client(session, "dynamodb")

@mock_dynamodb
def test_get_resource():
    auth = AWSAuth(
        {
            "aws_access_key_id": "foo",
            "aws_secret_access_key": "bar",
            "aws_default_region": "baz",
        }
    )
    session = auth.get_session()
    resource = auth.get_resource(session, "dynamodb")
