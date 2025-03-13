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

import os
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from acit import acit
from acit import gaql
from acit import resource_downloader
from google import auth
from google.ads.googleads import client


class TestAcit(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self._old_environ = os.environ.copy()
    self._logging_cm = self.enter_context(self.assertLogs())

  def tearDown(self):
    super().tearDown()
    for k in self._old_environ:
      os.environ[k] = self._old_environ[k]

  @mock.patch.object(resource_downloader, 'download_resources', autospec=True)
  @mock.patch.object(gaql, 'run_query', autospec=True)
  def test_login_customer_id(self, run_query, download_resources):
    ads_client = mock.create_autospec(client.GoogleAdsClient, instance=True)
    with mock.patch.object(
        client, 'GoogleAdsClient', return_value=ads_client, autospec=True
    ):
      download_resources.return_value = []
      run_query.return_value = []

      output_dir = self.create_tempdir()
      with flagsaver.as_parsed(
          customer_id=['123'],
          output=str(output_dir),
      ):
        acit.main(None)
        self.assertEqual('123', ads_client.login_customer_id)
        self.assertEqual('123', run_query.call_args.kwargs['customer_id'])

  @mock.patch.object(resource_downloader, 'download_resources', autospec=True)
  @mock.patch.object(gaql, 'run_query', autospec=True)
  def test_login_customer_id_with_subaccount(
      self, run_query, download_resources
  ):
    ads_client = mock.create_autospec(client.GoogleAdsClient, instance=True)
    with mock.patch.object(
        client, 'GoogleAdsClient', return_value=ads_client, autospec=True
    ):
      download_resources.return_value = []
      run_query.return_value = []

      output_dir = self.create_tempdir()
      with flagsaver.as_parsed(
          customer_id=['456:789'],
          output=str(output_dir),
      ):
        acit.main(None)
        self.assertEqual('456', ads_client.login_customer_id)
        self.assertEqual('789', run_query.call_args.kwargs['customer_id'])

  @parameterized.named_parameters(
      {
          'testcase_name': 'use_oauth',
          'client_id': 'abc',
          'client_secret': '123',
          'refresh_token': '!@#',
          'expect_is_adc': False,
      },
      {
          'testcase_name': 'no_client_id',
          'client_id': '',
          'client_secret': '123',
          'refresh_token': '!@#',
          'expect_is_adc': True,
      },
      {
          'testcase_name': 'no_client_secret',
          'client_id': 'abc',
          'client_secret': '',
          'refresh_token': '!@#',
          'expect_is_adc': True,
      },
      {
          'testcase_name': 'no_refresh_token',
          'client_id': 'abc',
          'client_secret': '123',
          'refresh_token': '',
          'expect_is_adc': True,
      },
  )
  @mock.patch.object(resource_downloader, 'download_resources', autospec=True)
  @mock.patch.object(gaql, 'run_query', autospec=True)
  @mock.patch.object(auth, 'default', wraps=auth.default)
  def test_credentials(
      self,
      mock_auth_default,
      run_query,
      download_resources,
      client_id,
      client_secret,
      refresh_token,
      expect_is_adc,
  ):
    os.environ['GOOGLE_ADS_CLIENT_ID'] = client_id
    os.environ['GOOGLE_ADS_CLIENT_SECRET'] = client_secret
    os.environ['GOOGLE_ADS_REFRESH_TOKEN'] = refresh_token

    ads_client = mock.create_autospec(client.GoogleAdsClient, instance=True)
    with mock.patch.object(
        client, 'GoogleAdsClient', return_value=ads_client, autospec=True
    ):
      download_resources.return_value = []
      run_query.return_value = []

      output_dir = self.create_tempdir()
      with flagsaver.as_parsed(
          output=str(output_dir),
      ):
        acit.main(None)
        if expect_is_adc:
          mock_auth_default.assert_called()
          self.assertTrue(
              any([
                  'application default' in output
                  for output in self._logging_cm.output
              ]),
              'use of application default credentials not logged',
          )
        else:
          mock_auth_default.assert_not_called()
          self.assertTrue(
              any(['oauth' in output for output in self._logging_cm.output]),
              'use of oauth credentials not logged',
          )


if __name__ == '__main__':
  absltest.main()
