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

from absl.testing import absltest

import apache_beam as beam
from apache_beam.testing import test_pipeline
from apache_beam.testing import util

from acit import product
from acit import create_base_tables


class CreateBaseTablesTest(absltest.TestCase):

  def test_combine_campaign_settings_works(self):
    self._combine_campaign_settings_test(
        campaigns=[{
            'customer': {'id': '123'},
            'campaign': {
                'id': '123',
                'shoppingSetting': {
                    'merchantId': '456',
                },
                'advertisingChannelType': 'something',
            },
        }],
        languages=[('123', {'language': 'English', 'is_targeted': True})],
        listing_scopes=[('123', [])],
        expected=[{
            'customer_id': '123',
            'campaign_id': '123',
            'merchant_id': '456',
            'languages': [{'language': 'English', 'is_targeted': True}],
            'campaign_type': 'something',
            'enable_local': False,
            'feed_label': '',
            'inventory_filter_dimensions': [],
            'sales_country': '',
        }],
    )

  def test_criteria_without_campaigns_race_condition(self):
    self._combine_campaign_settings_test(
        campaigns=[],
        languages=[('123', {'language': 'English', 'is_targeted': True})],
        listing_scopes=[('123', [])],
        expected=[],
    )

  def _combine_campaign_settings_test(
      self, campaigns, languages, listing_scopes, expected
  ):
    with test_pipeline.TestPipeline() as p:
      campaign_settings = p | 'Create campaign settings' >> beam.Create(
          campaigns
      )
      languages_by_campaign_id = p | 'Create Languages' >> beam.Create(
          languages
      )
      listing_scopes_by_campaign_id = (
          p | 'Create Listing Scopes' >> beam.Create(listing_scopes)
      )

      combined = create_base_tables.combine_campaign_settings(
          campaign_settings,
          languages_by_campaign_id,
          listing_scopes_by_campaign_id,
      )

      util.assert_that(combined, util.equal_to(expected))


if __name__ == '__main__':
  absltest.main()
