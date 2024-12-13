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
from absl.testing import flagsaver
from acit import acit
from acit import gaql
from acit import resource_downloader
from google.ads.googleads import client


class TestAcit(absltest.TestCase):

  @mock.patch.object(resource_downloader, 'download_resources', autospec=True)
  @mock.patch.object(gaql, 'run_query', autospec=True)
  @mock.patch.object(client.GoogleAdsClient, 'load_from_env', autospec=True)
  def test_login_customer_id(
      self, load_from_env, run_query, download_resources
  ):
    ads_client = mock.create_autospec(client.GoogleAdsClient)
    load_from_env.return_value = ads_client
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
  @mock.patch.object(client.GoogleAdsClient, 'load_from_env', autospec=True)
  def test_login_customer_id_with_subaccount(
      self, load_from_env, run_query, download_resources
  ):
    ads_client = mock.create_autospec(client.GoogleAdsClient)
    load_from_env.return_value = ads_client
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


if __name__ == '__main__':
  absltest.main()
