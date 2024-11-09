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
r"""Standalone script for generating an oauth playground link.

Usage:
   python acit/auth/oauth.py \
     path/to/client_secrets.json \
     content \
     adwords
"""

from collections.abc import Sequence
import json
import sys
from urllib import parse

_SCOPES_PREFIX = 'https://www.googleapis.com/auth/'

_OAUTH_PLAYGROUND_URL = 'https://developers.google.com/oauthplayground'

_STEP_1_URL = _OAUTH_PLAYGROUND_URL + '/#step1'


def get_secrets_dict(secrets_path: str) -> dict[str, str]:
  """Get the secrets dictionary from stored web credentials.

  Args:
    secrets_path: The path to the secrets file.

  Returns:
    A dictionary representing the secrets.
  """
  with open(secrets_path) as f:
    secrets = json.loads(f.read()).get('web', {})
  return secrets


def get_client_id_and_secret(secrets: dict[str, str]) -> tuple[str, str]:
  """Get the client ID and secret.

  Args:
    secrets: The secrets dict to parse

  Returns:
    A tuple of client_id and client_secret
  """
  client_id = secrets.get('client_id', '')
  client_secret = secrets.get('client_secret', '')
  return client_id, client_secret


def get_oauth_url(secrets_path: str, scopes: list[str]) -> str:
  """Get the OAuth Playground URL.

  Args:
    secrets_path: The path to the client secrets file, relative to invocation.
    scopes: The scopes to request.

  Returns:
    The fully-formed playground URL.

  Raises:
    AssertionError if any of the secrets are missing.
  """
  secrets = get_secrets_dict(secrets_path)

  client_id, client_secret = get_client_id_and_secret(secrets)
  if not client_id or not client_secret:
    raise ValueError('No client ID or secret found')

  redirect_uris = secrets.get('redirect_uris', [])
  if _OAUTH_PLAYGROUND_URL not in redirect_uris:
    raise ValueError('OAuth Playground not found in redirect URIs')

  # scopes are space-delimited
  scope_str = ' '.join([_SCOPES_PREFIX + scope for scope in scopes])

  query_params = {
      'oauthClientId': client_id,
      'oauthClientSecret': client_secret,
      'scopes': scope_str,
      'useDefaultOauthCred': 'checked',
  }

  query_str = parse.urlencode(query_params, quote_via=parse.quote)

  # OAuth Playground only prepopulates if I use an ampersand here
  return '{}&{}'.format(_STEP_1_URL, query_str)


def main(argv: Sequence[str]) -> None:
  print(argv)
  if len(argv) < 3:
    raise ValueError('Filename or scopes not provided')
  secrets_path = argv[1]
  scopes = [scope for scope in argv[2:]]
  playground_url = get_oauth_url(secrets_path, scopes)
  print('Please visit: ', playground_url)


if __name__ == '__main__':
  main(sys.argv)
