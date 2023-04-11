import logging
import os

import boto3


class AWSAuth:
    def __init__(self, config):
        # config for use environment variables
        if config.get("use_aws_env_vars"):
            self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
            self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            self.aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
            self.aws_profile = os.environ.get("AWS_PROFILE")
            self.aws_default_region = os.environ.get("AWS_DEFAULT_REGION")
        else:
            self.aws_access_key_id = config.get("aws_access_key_id")
            self.aws_secret_access_key = config.get("aws_secret_access_key")
            self.aws_session_token = config.get("aws_session_token")
            self.aws_profile = config.get("aws_profile")
            self.aws_default_region = config.get("aws_default_region")

        self.aws_endpoint_url = config.get("aws_endpoint_url")
        self.aws_assume_role_arn = config.get("aws_assume_role_arn")

        self.logger = logging.getLogger(__name__)

    def get_session(self):
        session = None
        # TODO: require region
        if (
            self.aws_access_key_id
            and self.aws_secret_access_key
            and self.aws_session_token
            and self.aws_default_region
        ):
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=self.aws_default_region,
            )
        elif (
            self.aws_access_key_id
            and self.aws_secret_access_key
            and self.aws_default_region
        ):
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_default_region,
            )
        elif self.aws_profile:
            session = boto3.Session(profile_name=self.aws_profile)
            self.logger.info("Using installed shared credentials file.")
        else:
            raise Exception("Explicit AWS auth not provided")

        if self.aws_assume_role_arn:
            if not session:
                raise Exception("Insufficient inputs for AWS Auth.")
            session = self._assume_role(session, self.aws_assume_role_arn)
        return session

    def _factory(self, aws_obj, service_name):
        if self.aws_endpoint_url:
            return aws_obj(
                service_name,
                endpoint_url=self.aws_endpoint_url,
            )
        else:
            return aws_obj(
                service_name,
            )

    def get_resource(self, session, service_name):
        return self._factory(session.resource, service_name)

    def get_client(self, session, service_name):
        return self._factory(session.client, service_name)

    def _assume_role(self, session, role_arn):
        # TODO: use for auto refresh https://github.com/benkehoe/aws-assume-role-lib
        sts_client = self.get_client(session, "sts")
        response = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="tap-dynamodb"
        )
        return boto3.Session(
            aws_access_key_id=response["Credentials"]["AccessKeyId"],
            aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
            aws_session_token=response["Credentials"]["SessionToken"],
        )
