# Copyright 2023 Google LLC
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
"""Main ACIT data downloader."""

from absl import app
from absl import flags
from absl import logging

from acit import gaql
from acit import resource_downloader

from google.ads.googleads import client

from googleapiclient import discovery

import google.auth

import os
import datetime
import json
import pathlib
import sys

_CUSTOMER_IDS = flags.DEFINE_multi_string(
    'customer_id',
    '',
    (
        'The customer ID to query. '
        'May be specified multiple times. '
        'Expands MCCs.'
    ),
)

_MERCHANT_CENTER_IDS = flags.DEFINE_multi_string(
    'merchant_id',
    '',
    ('The Merchant account ID. ' 'Expands Multi-client accounts.'),
)

# NOTE: Always add customer.id to a query for uniqueness.

# NOTE: Merchant Center FK has the form of channel:language:feed_label:item_id
# NOTE: Impressions will always be > 0 because this data is historical
_GAQL_SHOPPING_PERFORMANCE_VIEW = """
SELECT
  customer.id,
  segments.product_merchant_id,
  segments.product_channel,
  segments.product_language,
  segments.product_feed_label,
  segments.product_item_id,
  metrics.impressions,
  metrics.clicks,
  metrics.cost_micros,
  metrics.conversions,
  metrics.conversions_value
FROM shopping_performance_view
WHERE
  segments.date DURING LAST_7_DAYS
"""

# Non-PMax
# TODO: Evaluate SHOPPING_COMPARISON_LISTING_ADS
_GAQL_AD_GROUP_CRITERIA = """
SELECT
  customer.id,
  campaign.id,
  ad_group.id,
  ad_group_criterion.criterion_id,
  ad_group_criterion.negative,
  ad_group_criterion.status,
  ad_group_criterion.display_name,
  ad_group_criterion.type
FROM ad_group_criterion
WHERE
  ad_group_criterion.status = 'ENABLED'
  AND ad_group_criterion.negative = FALSE
  AND campaign.status = 'ENABLED'
  AND ad_group.status = 'ENABLED'
  AND ad_group.type IN ('SHOPPING_PRODUCT_ADS', 'SHOPPING_SMART_ADS')
"""

# Asset group - PMax only
# Asset group listing filter (No metrics)
# TODO: Will this need to be recursive due to parent filters?
#       Need to understand more.
# TODO: Check against 'UNSPECIFIED' product channel and condition.
# NOTE: product_item_id = offer_id
# NOTE: bidding_category = MC Google Product Category (<= 5)
# NOTE: custom_attribute = MC Custom Label (<= 5), 1-indexed
# NOTE: product_type = MC Product Category (<= 5), 1-indexed
# NOTE: type:UNIT_INCLUDED = case value is targeted leaf; path provides parents.

# NOTE: Channel Exclusivity isn't really a targeting thing.
# TODO: Filter only for Ads channels, not free listings or similar.

_GAQL_ASSET_GROUP_LISTING_FILTER = """
SELECT
  customer.id,
  campaign.id,
  asset_group.id,
  asset_group_listing_group_filter.id,
  asset_group_listing_group_filter.parent_listing_group_filter,
  asset_group_listing_group_filter.type,
  asset_group_listing_group_filter.case_value.product_brand.value,
  asset_group_listing_group_filter.case_value.product_item_id.value,
  asset_group_listing_group_filter.case_value.product_condition.condition,
  asset_group_listing_group_filter.case_value.product_channel.channel,
  asset_group_listing_group_filter.case_value.product_custom_attribute.index,
  asset_group_listing_group_filter.case_value.product_custom_attribute.value,
  asset_group_listing_group_filter.case_value.product_type.level,
  asset_group_listing_group_filter.case_value.product_type.value,
  asset_group_listing_group_filter.case_value.product_bidding_category.level,
  asset_group_listing_group_filter.case_value.product_bidding_category.id,
  asset_group_listing_group_filter.path
FROM asset_group_listing_group_filter
WHERE
  asset_group.status = 'ENABLED'
  AND campaign.status = 'ENABLED'
  AND asset_group_listing_group_filter.type IN ('UNIT_INCLUDED', 'UNIT_EXCLUDED')
"""

_ALL_GAQL = {
    'ad_group_criterion': _GAQL_AD_GROUP_CRITERIA,
    'asset_group_listing_filter': _GAQL_ASSET_GROUP_LISTING_FILTER,
    'shopping_performance_view': _GAQL_SHOPPING_PERFORMANCE_VIEW,
}

_ACIT_OUTPUT_DIR = '/tmp/acit'

_ACIT_ADS_PREFIX = 'acit_ads'
_ACIT_ADS_OUTPUT_DIR = 'ads'

_ACIT_MC_OUTPUT_DIR = 'merchant_center'

_ACIT_MC_RESOURCES = [
    # TODO: add MCA-specific resources
    'products',
    'productstatuses',
]


def main(_):
  now = datetime.datetime.today().isoformat()

  acit_output_dir = os.path.join(_ACIT_OUTPUT_DIR, now)
  acit_ads_output_dir = os.path.join(acit_output_dir, _ACIT_ADS_OUTPUT_DIR)
  acit_mc_output_dir = os.path.join(acit_output_dir, _ACIT_MC_OUTPUT_DIR)

  # Make sure paths exist
  ads_path = pathlib.Path(acit_ads_output_dir)
  ads_path.mkdir(parents=True, exist_ok=True)
  mc_path = pathlib.Path(acit_mc_output_dir)
  mc_path.mkdir(parents=True, exist_ok=True)

  # Download ads data
  logging.info('Loading Ads data...')
  for customer_id in _CUSTOMER_IDS.value:
    logging.info('Processing Customer ID %s' % customer_id)
    ads_client = client.GoogleAdsClient.load_from_storage()
    for resource, query in _ALL_GAQL.items():
      logging.info('...pulling resource %s...' % resource)
      output_dir = os.path.join(acit_ads_output_dir, customer_id, resource)
      pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
      gaql.run_query(
          query=query,
          ads_client=ads_client,
          customer_id=customer_id,
          prefix=f'{_ACIT_ADS_PREFIX}_{resource}',
          output_dir=output_dir,
      )
  logging.info('Done loading Ads data.')

  logging.info('Loading Merchant Center data...')
  # Technically, we already had creds from Ads. This is duplicative.
  #   In a perfect world, we use something like Secret Manager to retrieve
  #   OAuth client creds, Ads Dev tokens, and refresh tokens by account id.
  #   See https://developers.google.com/identity/protocols/oauth2/policies
  creds, _ = google.auth.default()
  # TODO: handle MCAs
  # TODO: break out Merchant Center logic into its own file
  for merchant_id in _MERCHANT_CENTER_IDS.value:
    logging.info('Processing Merchant Center ID %s...' % merchant_id)
    merchant_api = discovery.build('content', 'v2.1', credentials=creds)
    for resource in _ACIT_MC_RESOURCES:
      logging.info('...pulling resource %s...' % resource)
      output_file = os.path.join(
          acit_mc_output_dir, merchant_id, resource, 'rows.jsonlines'
      )
      pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)
      with open(output_file, 'wt') as f:
        for result in resource_downloader.download_resources(
            client=merchant_api,
            resource_name=resource,
            params={'merchantId': merchant_id},
            parent_resource='',
            parent_params={},
            resource_method='list',
            result_path='resources',
            metadata={'merchantId': merchant_id},
        ):
          print(json.dumps(result), file=f)
  logging.info('Done loading Merchant Center data.')


if __name__ == '__main__':
  app.run(main)
