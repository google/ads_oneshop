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

import textwrap

from absl.testing import absltest
from acit import register_app_secrets
from etils import epath


class RegisterAppSecretsTest(absltest.TestCase):

  def test_load_config(self):
    config_file = self.create_tempfile(content=textwrap.dedent("""\
              client_id: my client id
              client_secret: my client secret
              developer_token: my google ads developer token"""))
    config_path = epath.Path(config_file)
    config = register_app_secrets.load_application_secrets(config_path)
    self.assertEqual('my client id', config.client_id)
    self.assertEqual('my client secret', config.client_secret)
    self.assertEqual('my google ads developer token', config.developer_token)

  def test_load_config_missing_value(self):
    config_file = self.create_tempfile(content=textwrap.dedent("""\
              client_id: my client id
              developer_token: my google ads developer token"""))
    config_path = epath.Path(config_file)
    with self.assertRaisesRegex(ValueError, '[Ii]nvalid format.*are required'):
      register_app_secrets.load_application_secrets(config_path)

  def test_load_config_empty_file(self):
    config_file = self.create_tempfile(content='')
    config_path = epath.Path(config_file)
    with self.assertRaisesRegex(ValueError, '[Ii]nvalid format.*are required'):
      register_app_secrets.load_application_secrets(config_path)


if __name__ == '__main__':
  absltest.main()
