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
"""Store application secrets.

Stores client ID, client secret, and Google Ads developer token in Secret
Manager.

Loads secrets from known config files, which should *not* be checked into
version control.
"""

from collections.abc import Sequence
import dataclasses

from absl import app
from absl import flags
from absl import logging
from acit import secret
from acit.auth import oauth
from etils import epath
import yaml

_CLIENT_SECRETS_PATH = epath.DEFINE_path(
    'client_secrets_path',
    'client_secrets.json',
    'The path to the client secrets file',
)
_DEVELOPER_TOKEN_PATH = epath.DEFINE_path(
    'developer_token_path',
    'google_ads_developer_token.txt',
    'The path to the Google Ads developer token file.',
)
_REFRESH_TOKEN_PATH = epath.DEFINE_path(
    'refresh_token_path',
    'refresh_token.txt',
    'The path to the OAuth refresh token file.',
)
_REFRESH_TOKEN_SECRET_NAME = flags.DEFINE_string(
    'refresh_token_secret_name',
    'google_ads_refresh_token',
    'The name of the refresh token secret to save',
)


@dataclasses.dataclass
class ApplicationSecrets:
  """Common application secrets, not user-specific.

  Attributes:
    client_id: The Client ID.
    client_secret: The Client Secret.
    developer_token: The Google Ads Developer Token.
  """

  client_id: str = ''
  client_secret: str = ''
  developer_token: str = ''


def load_application_secrets(config_file: epath.Path) -> ApplicationSecrets:
  """Load Application Secrets from a given file.

  Args:
    config_file: The config file to load.

  Returns:
    The application secrets.

  Raises:
    ValueError: If the config file is in the wrong format.
  """
  config_content = config_file.read_text()
  docs = yaml.safe_load_all(config_content)
  for doc in docs:
    client_id = doc.get('client_id', '')
    client_secret = doc.get('client_secret', '')
    developer_token = doc.get('developer_token', '')
    if not (client_id and client_secret and developer_token):
      continue
    secrets = ApplicationSecrets(
        client_id=client_id,
        client_secret=client_secret,
        developer_token=developer_token,
    )
    return secrets
  raise ValueError(
      'Invalid format for config file. '
      'client_id, client_secret, and developer_token are required.'
  )


def store_application_secrets(
    client_secrets_path: epath.Path,
    developer_token_path: epath.Path,
    refresh_token_path: epath.Path,
    refresh_token_secret_name: str = 'google_ads_refresh_token',
) -> None:
  """Stores application secrets.

  Args:
    client_secrets_path: The path to the client secrets file.
    developer_token_path: The path to the developer token file.
    refresh_token_path: The path to the refresh token file.
    refresh_token_secret_name: The optional name for the refresh token secret.
        useful for multitenant deployments.
  """
  secrets = oauth.get_secrets_dict(str(client_secrets_path))
  client_id, client_secret = oauth.get_client_id_and_secret(secrets)
  with refresh_token_path.open() as f:
    refresh_token = f.read().strip()
  with developer_token_path.open() as f:
    developer_token = f.read().strip()
  manager = secret.SecretsManager.create_default()
  manager.store_secrets(client_id, 'google_ads_client_id')
  manager.store_secrets(client_secret, 'google_ads_client_secret')
  manager.store_secrets(developer_token, 'google_ads_developer_token')
  manager.store_secrets(refresh_token, refresh_token_secret_name)


def main(argv: Sequence[str]):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')
  store_application_secrets(
      client_secrets_path=_CLIENT_SECRETS_PATH.value,
      developer_token_path=_DEVELOPER_TOKEN_PATH.value,
      refresh_token_path=_REFRESH_TOKEN_PATH.value,
      refresh_token_secret_name=_REFRESH_TOKEN_SECRET_NAME.value,
  )
  logging.info('Updated application secrets.')


if __name__ == '__main__':
  app.run(main)
