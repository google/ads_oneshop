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
from googleapiclient import http

import google.auth

import os
import datetime
import json
import pathlib

import sys

from typing import Set

if sys.version_info < (3, 9, 0):
  # Required for union operators
  raise RuntimeError('Python 3.9 or greater required.')

ADS_API_VERSION = 'v14'


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
    'The Merchant account ID. Expands Multi-client accounts.',
)

_OUTPUT_DIR = flags.DEFINE_string(
    'output',
    '/tmp/acit',
    'The output directory for this data',
)

_ADMIN_RIGHTS = flags.DEFINE_boolean(
    'admin',
    True,
    'Whether to run against Merchant Center with admin privileges.',
)

# NOTE: Always add customer.id to a query for uniqueness.

# NOTE: Merchant Center FK has the form of channel:language:feed_label:item_id
# NOTE: Impressions will always be > 0 because this data is historical
_GAQL_SHOPPING_PERFORMANCE_VIEW = """
SELECT
  customer.id,
  campaign.id,
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

# WIP: need to query for each campaign type
# If neither feed label nor sales country are set, then
#   shopping campaigns target all feeds from an account.
# TODO: Check local result for PMax (default enabled)
_GAQL_CAMPAIGN_SETTINGS = """
SELECT
  customer.id,
  campaign.id,
  campaign.status,
  campaign.shopping_setting.campaign_priority,
  campaign.shopping_setting.enable_local,
  campaign.shopping_setting.feed_label,
  campaign.shopping_setting.merchant_id,
  campaign.shopping_setting.sales_country,
  campaign.status,
  campaign.advertising_channel_type,
  campaign.advertising_channel_sub_type
FROM campaign
WHERE
  campaign.status = 'ENABLED'
"""

# Non-PMax

# "Inventory Filters"
_GAQL_CAMPAIGN_CRITERIA = """
SELECT
  customer.id,
  campaign.id,
  campaign.status,
  campaign_criterion.status,
  campaign_criterion.type,
  campaign_criterion.listing_scope.dimensions,
  campaign_criterion.language.language_constant,
  campaign_criterion.negative
FROM campaign_criterion
WHERE
  campaign_criterion.status = 'ENABLED'
  AND campaign.status = 'ENABLED'
  AND campaign_criterion.type IN ('LISTING_SCOPE', 'LANGUAGE')
"""

_GAQL_AD_GROUP_CRITERIA = """
SELECT
  customer.id,
  campaign.id,
  ad_group.id,
  ad_group_criterion.criterion_id,
  ad_group_criterion.negative,
  ad_group_criterion.status,
  ad_group_criterion.display_name,
  ad_group_criterion.type,
  ad_group_criterion.listing_group.path,
  ad_group_criterion.listing_group.type
FROM ad_group_criterion
WHERE
  ad_group_criterion.status = 'ENABLED'
  AND campaign.status = 'ENABLED'
  AND ad_group.status = 'ENABLED'
  AND ad_group_criterion.type IN ('LISTING_GROUP')
  AND ad_group_criterion.listing_group.type = 'UNIT'
"""

# Asset group - PMax only
_GAQL_ASSET_GROUP_LISTING_FILTER = """
SELECT
  customer.id,
  campaign.id,
  asset_group.id,
  asset_group_listing_group_filter.id,
  asset_group_listing_group_filter.parent_listing_group_filter,
  asset_group_listing_group_filter.type,
  asset_group_listing_group_filter.path
FROM asset_group_listing_group_filter
WHERE
  asset_group.status = 'ENABLED'
  AND campaign.status = 'ENABLED'
  AND asset_group_listing_group_filter.type IN ('UNIT_INCLUDED', 'UNIT_EXCLUDED')
"""

_GAQL_LANGUAGE_CONSTANTS = """
SELECT
  language_constant.code,
  language_constant.name,
  language_constant.resource_name
FROM language_constant
WHERE
  language_constant.targetable = TRUE
"""

_GAQL_PRODUCT_CATEGORIES = """
SELECT
  product_bidding_category_constant.id,
  product_bidding_category_constant.localized_name
FROM product_bidding_category_constant
WHERE
  product_bidding_category_constant.status = 'ACTIVE'
  AND product_bidding_category_constant.language_code = 'en'
  AND product_bidding_category_constant.country_code = 'US'
"""

_ALL_GAQL = [
    ('campaign', _GAQL_CAMPAIGN_SETTINGS, gaql.QueryMode.LEAVES),
    ('campaign_criterion', _GAQL_CAMPAIGN_CRITERIA, gaql.QueryMode.LEAVES),
    ('ad_group_criterion', _GAQL_AD_GROUP_CRITERIA, gaql.QueryMode.LEAVES),
    (
        'asset_group_listing_filter',
        _GAQL_ASSET_GROUP_LISTING_FILTER,
        gaql.QueryMode.LEAVES,
    ),
    (
        'shopping_performance_view',
        _GAQL_SHOPPING_PERFORMANCE_VIEW,
        gaql.QueryMode.LEAVES,
    ),
    (
        'language_constant',
        _GAQL_LANGUAGE_CONSTANTS,
        gaql.QueryMode.SINGLE,
    ),
    (
        'product_category',
        _GAQL_PRODUCT_CATEGORIES,
        gaql.QueryMode.SINGLE,
    ),
]

_ACIT_ADS_PREFIX = 'acit_ads'
_ACIT_ADS_OUTPUT_DIR = 'ads'

_ACIT_MC_OUTPUT_DIR = 'merchant_center'

# Leaf-only resources
_ACIT_MC_RESOURCES = [
    'products',
    'productstatuses',
]

_ACIT_MC_ACCOUNT_RESOURCE = 'accounts'
_ACIT_MC_SHIPPINGSETTINGS_RESOURCE = 'shippingsettings'

# Rolled-down settings.
# Each resultant file will have exactly one entry.
# If that entry is from an MCA, it will have a 'children' key.
_ACIT_ACCOUNT_RESOURCES = [
    _ACIT_MC_ACCOUNT_RESOURCE,
]

# Additional resources which are only available to admins.
_ACIT_ACCOUNT_ADMIN_RESOURCES = [
    'liasettings',
    _ACIT_MC_SHIPPINGSETTINGS_RESOURCE,
]


def main(_):
  now = datetime.datetime.today().isoformat()

  acit_output_dir = os.path.join(_OUTPUT_DIR.value, now)
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
    ads_client = client.GoogleAdsClient.load_from_storage(
        version=ADS_API_VERSION
    )
    ads_client.login_customer_id = customer_id
    for resource, query, mode in _ALL_GAQL:
      logging.info('...pulling resource %s...' % resource)
      output_dir = os.path.join(acit_ads_output_dir, customer_id, resource)
      pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
      gaql.run_query(
          query=query,
          ads_client=ads_client,
          customer_id=customer_id,
          prefix=f'{_ACIT_ADS_PREFIX}_{resource}',
          output_dir=output_dir,
          query_mode=mode,
      )
  logging.info('Done loading Ads data.')

  # TODO: break out Merchant Center logic into its own file
  logging.info('Loading Merchant Center data...')
  # Technically, we already had creds from Ads. This is duplicative.
  #   In a perfect world, we use something like Secret Manager to retrieve
  #   OAuth client creds, Ads Dev tokens, and refresh tokens by account id.
  #   See https://developers.google.com/identity/protocols/oauth2/policies
  creds, _ = google.auth.default()

  # Before we can do anything, we need to know what type of accounts we're
  # dealing with.

  input_ids = set(_MERCHANT_CENTER_IDS.value)
  # Preload Merchant Center authinfo so we know which accounts (for this
  # user) are top-level.
  merchant_api = discovery.build('content', 'v2.1', credentials=creds)
  # The sets of accessible aggregators and standlone accounts
  aggregator_ids: Set[str] = set()
  standalone_ids: Set[str] = set()
  # All leaf accounts we will actually process
  leaf_ids: Set[str] = set()

  leaf_to_parent = {}

  for result in resource_downloader.download_resources(
      client=merchant_api,
      resource_name='accounts',
      params={},
      parent_resource='',
      parent_params={},
      resource_method='authinfo',
      result_path='',
      metadata={},
      is_scalar=True,
  ):
    for account_identifier in result.get('accountIdentifiers', []):
      # Users may only be present in one account.
      # Aggregator IDs are optional in leaves.
      # We must check for merchantId first.
      if 'merchantId' in account_identifier:
        standalone_ids.add(account_identifier['merchantId'])
      else:
        aggregator_ids.add(account_identifier['aggregatorId'])

  # Decide which resources to pull
  acit_account_resources = _ACIT_ACCOUNT_RESOURCES
  if _ADMIN_RIGHTS.value:
    acit_account_resources += _ACIT_ACCOUNT_ADMIN_RESOURCES

  # Top-level settings must roll down  (if they exist) from MCAs.
  # Top-level settings include image enhancement, LIA, Ads links, etc
  # This significantly complicates the data model (especially for Ads links)
  for aggregator_id in aggregator_ids.intersection(input_ids):
    for name in acit_account_resources:
      if name == _ACIT_MC_SHIPPINGSETTINGS_RESOURCE:
        # This shippingsettings.list is failing. Use get below.
        continue
      logging.info('Fetching account-level resource %s...' % name)
      for parent in resource_downloader.download_resources(
          client=merchant_api,
          resource_name=name,
          # Required to duplicate here
          params={'merchantId': aggregator_id, 'accountId': aggregator_id},
          parent_resource='',
          parent_params={},
          resource_method='get',
          result_path='',
          metadata={'accountId': aggregator_id},
          is_scalar=True,
      ):
        children = []
        # This key will only be set on MCA account-level metrics
        parent['children'] = children

        logging.info('Fetching subaccount resources %s...' % name)
        for child in resource_downloader.download_resources(
            client=merchant_api,
            resource_name=name,
            params={'merchantId': aggregator_id},
            parent_resource='',
            parent_params={},
            resource_method='list',
            result_path='resources',
            metadata={'parentId': aggregator_id},
        ):
          children.append(child)
          if name == _ACIT_MC_ACCOUNT_RESOURCE:
            leaf_to_parent[child['id']] = aggregator_id
            leaf_ids.add(child['id'])

        # Wait until the end so the parent has all children
        output_file = os.path.join(
            acit_mc_output_dir, aggregator_id, name, 'rows.jsonlines'
        )
        pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'wt') as f:
          print(json.dumps(parent), file=f)

  for account_id in leaf_ids | (standalone_ids & input_ids):
    logging.info('Processing Merchant Center ID %s...' % account_id)
    # We need account-level resources
    for resource in acit_account_resources:
      # TODO(b/305301891): Remove after shippingsettings.list is fixed.
      if (
          account_id in standalone_ids
          or resource == _ACIT_MC_SHIPPINGSETTINGS_RESOURCE
      ):
        logging.info(
            '...pulling standalone account-level resource %s...' % resource
        )
        output_file = os.path.join(
            acit_mc_output_dir, account_id, resource, 'rows.jsonlines'
        )
        pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'wt') as f:
          try:
            for result in resource_downloader.download_resources(
                client=merchant_api,
                resource_name=resource,
                params={
                    'merchantId': leaf_to_parent.get(account_id, account_id),
                    'accountId': account_id,
                },
                parent_resource='',
                parent_params={},
                resource_method='get',
                result_path='',
                metadata={'accountId': account_id},
                is_scalar=True,
            ):
              print(json.dumps(result), file=f)
          except http.HttpError as e:
            if (
                str(e.status_code) == '404'
                and resource == _ACIT_MC_SHIPPINGSETTINGS_RESOURCE
            ):
              logging.warning(
                  (
                      'Unable to find shipping settings for account %s. '
                      "It's possible no such setting exists."
                  )
                  % account_id
              )
            else:
              raise e

    for resource in _ACIT_MC_RESOURCES:
      logging.info('...pulling resource %s...' % resource)
      output_file = os.path.join(
          acit_mc_output_dir, account_id, resource, 'rows.jsonlines'
      )
      pathlib.Path(output_file).parent.mkdir(parents=True, exist_ok=True)
      with open(output_file, 'wt') as f:
        for result in resource_downloader.download_resources(
            client=merchant_api,
            resource_name=resource,
            # Yes, this is inconsistent with the rest of the API.
            params={'merchantId': account_id},
            parent_resource='',
            parent_params={},
            resource_method='list',
            result_path='resources',
            metadata={'accountId': account_id},
        ):
          print(json.dumps(result), file=f)

  unprocessed = input_ids - (leaf_ids | standalone_ids | aggregator_ids)
  if unprocessed:
    logging.warn(
        (
            'This credential does not have direct acccess to the following '
            'input account(s): %s. Some data may be missing. '
        )
        % ' ,'.join(unprocessed)
    )
  logging.info('Done loading Merchant Center data.')


if __name__ == '__main__':
  app.run(main)
