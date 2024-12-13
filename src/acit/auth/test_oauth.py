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

import json
import os
# from unittest import mock

from absl.testing import absltest
from acit.auth import oauth


class OAuthTest(absltest.TestCase):

  def test_get_oauth_url(self):
    client_creds = {
        'client_id': 'abc',
        'client_secret': '123',
        'redirect_uris': [
            'https://developers.google.com/oauthplayground',
        ],
    }

    secrets_content = {'web': client_creds}
    secrets = self.create_tempfile(content=json.dumps(secrets_content))
    secrets_path = os.fspath(secrets)
    scopes = ['first_scope', 'second_scope']

    expected = (
        'https://developers.google.com/oauthplayground/#step1'
        '&oauthClientId=abc'
        '&oauthClientSecret=123'
        '&scopes='
        'https%3A%2F%2Fwww.googleapis.com%2Fauth%2Ffirst_scope'
        '%20https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fsecond_scope'
        '&useDefaultOauthCred=checked'
    )

    actual = oauth.get_oauth_url(secrets_path, scopes)

    self.assertUrlEqual(expected, actual)


if __name__ == '__main__':
  absltest.main()
