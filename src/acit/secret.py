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
"""Abstract secret management."""

from typing import cast

from google import auth
from google.auth import credentials as _credentials
from google.cloud import secretmanager
import google_crc32c  # type: ignore


class SecretNotFoundError(ValueError):
  """Represents a missing secret."""


class SecretsManager:
  """Secret Manager facade."""

  def __init__(
      self, project_id: str, client: secretmanager.SecretManagerServiceClient
  ):
    self._project_id = project_id
    self._client = client

  @staticmethod
  def create_default() -> 'SecretsManager':
    """Creates a default instance using Application Default Credentials."""
    creds, project_id = auth.default()
    client = secretmanager.SecretManagerServiceClient(
        credentials=cast(_credentials.Credentials, creds)
    )
    manager = SecretsManager(cast(str, project_id), client)
    return manager

  def _get_secret_by_name(self, secret_name: str = '') -> secretmanager.Secret:
    """Loads a secret resource by its name, if one exists."""
    secret_name = secret_name.strip()
    if not secret_name:
      raise ValueError('secret_name missing')

    list_secrets_request = secretmanager.ListSecretsRequest(
        parent=secretmanager.SecretManagerServiceClient.common_project_path(
            self._project_id
        ),
        filter='name:%s' % (secret_name),
    )
    secrets = self._client.list_secrets(list_secrets_request)

    secret = next(iter(secrets), None)
    if not secret:
      raise SecretNotFoundError(
          'No secret found with the given name, %s.' % secret_name
      )
    return secret

  def store_secrets(
      self, payload: str, secret_name: str
  ) -> secretmanager.SecretVersion:
    """Store secrets in Secret Manager.

    Args:
      payload: The payload to store.
      secret_name: The name of the Secret Manager secret.

    Returns:
      The secret version.
    """

    try:
      secret = self._get_secret_by_name(secret_name)
    except SecretNotFoundError:
      secret_name = secret_name.strip()
      parent = secretmanager.SecretManagerServiceClient.common_project_path(
          self._project_id
      )
      secret_spec = secretmanager.Secret(
          replication=secretmanager.Replication(
              automatic=secretmanager.Replication.Automatic()
          )
      )
      create_request = secretmanager.CreateSecretRequest(
          parent=parent,
          secret_id=secret_name,
          secret=secret_spec,
      )
      secret = self._client.create_secret(create_request)

    # Secret manager won't let us store empty secrets directly
    secret_bytes = (payload or ' ').encode('UTF-8')
    crc32c = google_crc32c.Checksum()
    crc32c.update(secret_bytes)
    secret_version = self._client.add_secret_version(
        secretmanager.AddSecretVersionRequest(
            parent=secret.name,
            payload=secretmanager.SecretPayload(
                data=secret_bytes, data_crc32c=int(crc32c.hexdigest(), 16)
            ),
        )
    )
    return secret_version

  def get_secrets(self, secret_name: str = '') -> str:
    """Get secrets from secret manager.

    Args:
      secret_name: The name of the Secret Manager secret.

    Returns:
      The decoded  secrets.

    Raises:
      ValueError: If the secret name is empty or does not exist.
    """

    secret = self._get_secret_by_name(secret_name)

    secret_versions = self._client.list_secret_versions(
        secretmanager.ListSecretVersionsRequest(
            parent=secret.name,
        )
    )
    # Secret Manager returns the most recent version first
    secret_version = next(iter(secret_versions), None)
    if not secret_version:
      raise ValueError('No secret versions found for secret: %s' % secret_name)

    response = self._client.access_secret_version(
        secretmanager.AccessSecretVersionRequest(
            name=secret_version.name,
        )
    )

    crc32c = google_crc32c.Checksum()
    crc32c.update(response.payload.data)

    assert response.payload.data_crc32c == int(crc32c.hexdigest(), 16)

    payload = response.payload.data.decode('UTF-8')
    return payload
