import logging

from tap_dynamodb.aws_auth import AWSAuth


class AWSBase:
    def __init__(self, config, service_name):
        self._service_name = service_name
        self._config = config
        self._client = None
        self._resource = None
        self.logger = logging.getLogger(__name__)

    @property
    def client(self):
        if self._client:
            return self._client
        else:
            auth_obj = AWSAuth(self._config)
            session = auth_obj.get_session()
            self._client = auth_obj.get_resource(session, self._service_name)
            return self._client

    @property
    def resource(self):
        if self._resource:
            return self._resource
        else:
            auth_obj = AWSAuth(self._config)
            session = auth_obj.get_session()
            self._resource = auth_obj.get_client(session, self._service_name)
            return self._resource
