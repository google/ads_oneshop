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

import os
import sys
from typing import Set

from absl import app
from absl import flags
from absl import logging
from acit import constants
from acit import gaql
from acit import merchant_accounts
from acit import merchant_lia
from acit import merchant_products
from acit import merchant_programs
from acit import merchant_promotions
from acit import merchant_reports
from acit import merchant_returns
from acit import merchant_shipping
from etils import epath
from google import auth
from google.ads.googleads import client
from google.auth import credentials
from google.oauth2 import credentials as oauth_credentials


if sys.version_info < (3, 9, 0):
  # Required for union operators
  raise RuntimeError('Python 3.9 or greater required.')

ADS_API_VERSION = 'v22'

_OAUTH_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token'

_CUSTOMER_IDS = flags.DEFINE_multi_string(
    'customer_id',
    '',
    (
        'The customer ID to query. May be specified multiple times. Expands'
        ' MCCs. Accepts "login_customer_id:customer_id" if a separate login'
        ' customer ID is required.'
    ),
)

_MERCHANT_CENTER_IDS = flags.DEFINE_multi_string(
    'merchant_id',
    '',
    'The Merchant account ID. Expands Multi-client accounts.',
)

_OUTPUT_DIR = epath.DEFINE_path(
    'output',
    '/tmp/acit',
    'The output directory for this data',
)

_ADMIN_RIGHTS = flags.DEFINE_boolean(
    'admin',
    True,
    'Whether to run against Merchant Center with admin privileges.',
)

_VALIDATE_ONLY = flags.DEFINE_boolean(
    'validate_only', False, 'Whether to validate GAQL queries only.'
)

_MC_MAX_WORKERS = flags.DEFINE_integer(
    'mc_max_workers',
    constants.DEFAULT_MAX_WORKERS,
    (
        'Max concurrent worker threads used by each Merchant API v1 '
        'ingestion stage (accounts, products, LIA, shipping, programs, '
        'promotions, returns, reports). Passed explicitly to every stage so '
        'they stay consistent with each other.'
    ),
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
  segments.date DURING LAST_30_DAYS
"""

# WIP: need to query for each campaign type
# If feed label is not set, then shopping campaigns
# target all feeds from an account.
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
  AND asset_group_listing_group_filter.type
    IN ('UNIT_INCLUDED', 'UNIT_EXCLUDED')
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
  product_category_constant.category_id,
  product_category_constant.localizations
FROM product_category_constant
WHERE
  product_category_constant.state = 'ENABLED'
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

_ACIT_ADS_OUTPUT_DIR = flags.DEFINE_string(
    'ads_output_dir',
    'ads',
    'The subdirectory in the output directory for Ads data.',
)

_ACIT_MC_OUTPUT_DIR = flags.DEFINE_string(
    'mc_output_dir',
    'merchant_center',
    'The subdirectory in the output directory for Merchant Center data.',
)

# NOTE: All Merchant Center resources are pulled from the
# Merchant API (stable v1). We query sub-accounts and standalone
# accounts directly using the native v1 shape.
# Accounts and shipping setting requests are admin-gated
# matching existing permissions.


def _get_credentials() -> credentials.Credentials:
  refresh_token = os.environ.get('GOOGLE_ADS_REFRESH_TOKEN', '').strip()
  client_id = os.environ.get('GOOGLE_ADS_CLIENT_ID', '').strip()
  client_secret = os.environ.get('GOOGLE_ADS_CLIENT_SECRET', '').strip()
  if not (refresh_token and client_id and client_secret):
    logging.info('Using application default credentials')
    creds, _ = auth.default()
    assert isinstance(creds, credentials.Credentials)
    return creds
  else:
    logging.info('Using oauth credentials')
    return oauth_credentials.Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri=_OAUTH_TOKEN_ENDPOINT,
        client_id=client_id,
        client_secret=client_secret,
    )


def _parse_login_customer_ids(customer_ids: list[str]) -> list[tuple[str, str]]:
  """Extracts login_customer_id:customer_id pairs from input.

  If no `:` delimiter is given, assume the customer ID is a login customer ID.

  Args:
    customer_ids: The input list of customers to fetch, and how to do so.

  Returns:
      A login customer ID, customer ID tuple. May be identical if no login
      Customer ID given.
  """
  login_cid_pairs = []
  for customer_id in customer_ids:
    login_cid, *rest = customer_id.split(':')
    customer_id = rest[0] if rest else login_cid
    login_cid_pairs.append((login_cid, customer_id))
  return login_cid_pairs


def main(_):
  creds = _get_credentials()
  developer_token = os.environ.get('GOOGLE_ADS_DEVELOPER_TOKEN')
  if _VALIDATE_ONLY.value:
    ads_client = client.GoogleAdsClient(
        credentials=creds,
        developer_token=developer_token,
        version=ADS_API_VERSION,
    )
    login_customer_id, customer_id = next(
        iter(_parse_login_customer_ids(_CUSTOMER_IDS.value))
    )
    ads_client.login_customer_id = login_customer_id
    for resource, query, mode in _ALL_GAQL:
      gaql.run_query(
          query=query,
          ads_client=ads_client,
          customer_id=customer_id,
          validate_only=True,
          use_test_accounts=gaql.USE_TEST_ACCOUNTS.value,
      )
    return

  acit_output_dir = _OUTPUT_DIR.value

  # Clear path
  acit_output_dir.rmtree(missing_ok=True)
  acit_output_dir.mkdir(parents=True)

  ads_path = acit_output_dir / _ACIT_ADS_OUTPUT_DIR.value
  mc_path = acit_output_dir / _ACIT_MC_OUTPUT_DIR.value

  # Make sure paths exist
  ads_path.mkdir()
  mc_path.mkdir()

  # Download ads data
  logging.info(
      'Ads YAML: %s',
      os.getenv('GOOGLE_ADS_CONFIGURATION_FILE_PATH', 'Not set'),
  )
  logging.info('Loading Ads data...')
  # Only load constants once
  constants_gaql = iter(
      [query for query in _ALL_GAQL if query[2] == gaql.QueryMode.SINGLE]
  )
  accounts_gaql = [
      query for query in _ALL_GAQL if query[2] != gaql.QueryMode.SINGLE
  ]
  for login_customer_id, customer_id in _parse_login_customer_ids(
      _CUSTOMER_IDS.value
  ):
    logging.info('Processing Customer ID %s', customer_id)
    ads_client = client.GoogleAdsClient(
        credentials=creds,
        developer_token=developer_token,
        version=ADS_API_VERSION,
    )
    ads_client.login_customer_id = login_customer_id
    # constants_gaql will be empty on subsequent invocations
    for resource, query, mode in accounts_gaql + [g for g in constants_gaql]:
      logging.info('...pulling resource %s...', resource)
      # NOTE: These directories were previously sharded on login account ID.
      #
      # Since BQ load only supports a single wildcard, we can't use directory
      # sharding on the MCC here. Login accounts are run sequentially. But if
      # this is ever changed, there is a possibility that two login accounts
      # could write to the same child account, causing a race condition.
      output_dir = ads_path / 'all' / resource
      output_dir.mkdir(parents=True, exist_ok=True)
      gaql.run_query(
          query=query,
          ads_client=ads_client,
          customer_id=customer_id,
          output_dir=str(output_dir),
          query_mode=mode,
          use_simple_filename=True,
          use_test_accounts=gaql.USE_TEST_ACCOUNTS.value,
      )
  logging.info('Done loading Ads data.')

  # TODO: break out Merchant Center logic into its own file
  logging.info('Loading Merchant Center data...')

  # Before we can do anything, we need to know what type of accounts we're
  # dealing with.

  input_ids = set(_MERCHANT_CENTER_IDS.value)

  # Fetch account topology (aggregation structure and standalone accounts) via
  # Merchant API v1 Accounts service.
  aggregator_ids, standalone_ids, leaf_to_parent = (
      merchant_accounts.download_accounts(
          creds, input_ids, mc_path, max_workers=_MC_MAX_WORKERS.value)
  )
  # All leaf (sub-) accounts we will actually process for product-level data.
  leaf_ids: Set[str] = set(leaf_to_parent)

  product_account_ids = leaf_ids | (standalone_ids & input_ids)

  # Download products data for all accessible leaf and standalone accounts.
  merchant_products.download_products(
      creds, product_account_ids, mc_path, max_workers=_MC_MAX_WORKERS.value)

  # Fetch LIA / omnichannel settings directly for each sub-account

  if _ADMIN_RIGHTS.value:
    merchant_lia.download_omnichannel_settings(
        creds, product_account_ids, mc_path, max_workers=_MC_MAX_WORKERS.value)

  # Fetch shipping settings directly for each sub-account (admin-gated).
  if _ADMIN_RIGHTS.value:
    merchant_shipping.download_shipping_settings(
        creds, product_account_ids, mc_path, max_workers=_MC_MAX_WORKERS.value)
    merchant_programs.download_programs(
        creds, product_account_ids, mc_path, max_workers=_MC_MAX_WORKERS.value
    )
    merchant_returns.download_returns(
        creds, product_account_ids, mc_path, max_workers=_MC_MAX_WORKERS.value
    )
    merchant_promotions.download_promotions(
        creds, product_account_ids, mc_path, max_workers=_MC_MAX_WORKERS.value
    )
    merchant_reports.download_reports(
        creds, product_account_ids, mc_path, max_workers=_MC_MAX_WORKERS.value
    )

  unprocessed = input_ids - (leaf_ids | standalone_ids | aggregator_ids)
  if unprocessed:
    logging.warn(
        'This credential does not have direct access to the following '
        'input account(s): %s. Some data may be missing. ',
        ','.join(unprocessed),
    )
  logging.info('Done loading Merchant Center data.')


if __name__ == '__main__':
  app.run(main)
