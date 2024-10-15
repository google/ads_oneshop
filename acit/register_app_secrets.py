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

Loads secrets from a known config file, which should *not* be checked into
version control.
"""

import dataclasses

from etils import epath
import yaml


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
