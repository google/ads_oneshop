# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: mode=test

from unittest import mock

from absl.testing import absltest
from acit import secret
from google.cloud import secretmanager
import google_crc32c  # type: ignore


class SecretsManagerTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._service = mock.Mock(
        secretmanager.SecretManagerServiceClient, auto_spec=True
    )
    self._project_id = 'my_project'
    self._secret_name = 'my_secret'
    self._manager = secret.SecretsManager(self._project_id, self._service)
    self._secret = secretmanager.Secret(
        name=f'projects/{self._project_id}/secrets/{self._secret_name}'
    )
    self._secret_version = secretmanager.SecretVersion(
        name=f'projects/{self._project_id}/secrets/{self._secret_name}/versions/1'
    )
    self._payload = 'my payload'
    self._checksum = 8
    self._bad_checksum = 9
    self._list_secrets_request = secretmanager.ListSecretsRequest(
        parent=secretmanager.SecretManagerServiceClient.common_project_path(
            self._project_id
        ),
        filter='name:%s' % (self._secret_name),
    )
    secret_spec = secretmanager.Secret(
        replication=secretmanager.Replication(
            automatic=secretmanager.Replication.Automatic()
        )
    )
    self._create_secret_request = secretmanager.CreateSecretRequest(
        parent=f'projects/{self._project_id}',
        secret_id=self._secret_name,
        secret=secret_spec,
    )
    self._access_secret_version_response = (
        secretmanager.AccessSecretVersionResponse({
            'payload': {
                'data': self._payload.encode('utf-8'),
                'data_crc32c': self._checksum,
            }
        })
    )

  def test_application_secret_manager_round_trip(self):
    self._service.list_secrets.return_value = [self._secret]
    self._service.add_secret_version.return_value = self._secret_version
    self._service.list_secret_versions.return_value = [self._secret_version]

    self._service.access_secret_version.return_value = (
        secretmanager.AccessSecretVersionResponse({
            'payload': {
                'data': self._payload.encode('utf-8'),
                'data_crc32c': self._checksum,
            }
        })
    )

    self._manager.store_secrets(self._payload, self._secret_name)
    with mock.patch.object(google_crc32c, 'Checksum', autospec=True) as cls:
      cls.return_value.hexdigest.return_value = str(self._checksum)

      actual = self._manager.get_secrets(self._secret_name)
      self.assertEqual(self._payload, actual)

  def test_secrets_manager_store_creates_secret_if_not_exists(self):
    self._service.list_secrets.return_value = []
    self._service.create_secret.return_value = self._secret
    self._service.add_secret_version.return_value = self._secret_version

    actual = self._manager.store_secrets(self._payload, self._secret_name)

    self.assertEqual(actual, self._secret_version)

    self._service.list_secrets.assert_called_once_with(
        self._list_secrets_request
    )

    self._service.create_secret.assert_called_once_with(
        self._create_secret_request
    )

    self._service.add_secret_version.assert_called_once()
    add_secret_version_request = (
        self._service.add_secret_version.call_args.args[0]
    )
    self.assertEqual(add_secret_version_request.parent, self._secret.name)
    self.assertEqual(
        add_secret_version_request.payload.data, self._payload.encode('UTF-8')
    )

  def test_secrets_manager_store_updates_secret_if_exists(self):
    self._service.list_secrets.return_value = [self._secret]
    self._service.add_secret_version.return_value = self._secret_version

    _ = self._manager.store_secrets(self._payload, self._secret_name)

    self._service.create_secret.assert_not_called()
    self._service.add_secret_version.assert_called_once()

  def test_secrets_manager_get_raises_value_error_if_not_exists(self):
    self._service.list_secrets.return_value = [self._secret]
    self._service.list_secret_versions.return_value = []

    with self.assertRaisesRegex(
        ValueError,
        f'[Nn]o secret versions found for secret.*{self._secret_name}',
    ):
      _ = self._manager.get_secrets(self._secret_name)

    self._service.list_secrets.assert_called_once_with(
        self._list_secrets_request
    )
    self._service.list_secret_versions.assert_called_once_with(
        secretmanager.ListSecretVersionsRequest(
            parent=self._secret.name,
        )
    )

  def test_secrets_manager_get_raises_assertion_error_bad_checksum(self):
    self._service.list_secrets.return_value = [self._secret]
    self._service.list_secret_versions.return_value = [self._secret_version]
    self._access_secret_version_response.payload.data_crc32c = self._checksum
    self._service.access_secret_version.return_value = (
        self._access_secret_version_response
    )

    with mock.patch.object(google_crc32c, 'Checksum', autospec=True) as cls:
      cls.return_value.hexdigest.return_value = str(self._bad_checksum)

      with self.assertRaises(AssertionError):
        _ = self._manager.get_secrets(self._secret_name)

    self._service.list_secrets.assert_called_once_with(
        self._list_secrets_request
    )
    self._service.list_secret_versions.assert_called_once_with(
        secretmanager.ListSecretVersionsRequest(
            parent=self._secret.name,
        )
    )
    self._service.access_secret_version.assert_called_once_with(
        secretmanager.AccessSecretVersionRequest(
            name=self._secret_version.name,
        )
    )

  def test_secrets_manager_get_secret(self):
    self._service.list_secrets.return_value = [self._secret]
    self._service.list_secret_versions.return_value = [self._secret_version]
    self._access_secret_version_response.payload.data_crc32c = self._checksum
    self._service.access_secret_version.return_value = (
        self._access_secret_version_response
    )

    with mock.patch.object(google_crc32c, 'Checksum', autospec=True) as cls:
      cls.return_value.hexdigest.return_value = str(self._checksum)

      actual = self._manager.get_secrets(self._secret_name)

    self.assertEqual(actual, self._payload)


if __name__ == '__main__':
  absltest.main()
