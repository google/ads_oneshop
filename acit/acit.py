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

from concurrent import futures
import json
import multiprocessing as mp
import os
import sys
from typing import Set, Any

from absl import app
from absl import flags
from absl import logging
from acit import gaql
from acit import resource_downloader
from etils import epath
from google.ads.googleads import client
from google.oauth2 import credentials
from googleapiclient import discovery
from googleapiclient import http


if sys.version_info < (3, 9, 0):
  # Required for union operators
  raise RuntimeError('Python 3.9 or greater required.')

ADS_API_VERSION = 'v17'

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
# If feed label is not set, then shopping campaigns target all feeds from an account.
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


def _get_merchant_center_api() -> Any:
  """Get Merchant Center API client. Factory.

  Reuses API credentials from Google Ads.

  Returns:
    The Merchant Center API client.
  """

  refresh_token = os.environ.get('GOOGLE_ADS_REFRESH_TOKEN')
  client_id = os.environ.get('GOOGLE_ADS_CLIENT_ID')
  client_secret = os.environ.get('GOOGLE_ADS_CLIENT_SECRET')
  creds = credentials.Credentials(
      token=None,
      refresh_token=refresh_token,
      token_uri=_OAUTH_TOKEN_ENDPOINT,
      client_id=client_id,
      client_secret=client_secret,
  )

  merchant_api = discovery.build('content', 'v2.1', credentials=creds)
  return merchant_api


def _pull_standalone_account_resource(
    acit_mc_output_dir, parent_id, account_id, resource
) -> None:
  merchant_api = _get_merchant_center_api()
  logging.info('...pulling standalone account-level resource %s...' % resource)
  output_file = (
      epath.Path(acit_mc_output_dir) / account_id / resource / 'rows.jsonlines'
  )
  output_file.parent.mkdir(parents=True, exist_ok=True)
  with output_file.open(mode='w') as f:
    try:
      for result in resource_downloader.download_resources(
          client=merchant_api,
          resource_name=resource,
          params={
              'merchantId': parent_id,
              'accountId': account_id,
          },
          parent_resource='',
          parent_params={},
          resource_method='get',
          result_path='',
          metadata={'accountId': account_id},
          is_scalar=True,
      ):
        d = {
            'settings': result,
            'children': [],
        }
        print(json.dumps(d), file=f)
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


def _pull_leaf_collection(acit_mc_output_dir, account_id, resource):
  merchant_api = _get_merchant_center_api()
  logging.info('...pulling resource %s...' % resource)
  output_file = (
      epath.Path(acit_mc_output_dir) / account_id / resource / 'rows.jsonlines'
  )
  output_file.parent.mkdir(parents=True, exist_ok=True)
  with output_file.open(mode='w') as f:
    for result in resource_downloader.download_resources(
        client=merchant_api,
        resource_name=resource,
        # Yes, 'merchantId' is inconsistent with the rest of the API.
        params={'merchantId': account_id, 'maxResults': 250},
        parent_resource='',
        parent_params={},
        resource_method='list',
        result_path='resources',
        metadata={'accountId': account_id},
    ):
      print(json.dumps(result), file=f)


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
  if _VALIDATE_ONLY.value:
    ads_client = client.GoogleAdsClient.load_from_env(version=ADS_API_VERSION)
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

  ads_path = acit_output_dir / _ACIT_ADS_OUTPUT_DIR
  mc_path = acit_output_dir / _ACIT_MC_OUTPUT_DIR

  # Make sure paths exist
  ads_path.mkdir()
  mc_path.mkdir()

  # Download ads data
  logging.info(
      'Ads YAML: %s'
      % os.getenv('GOOGLE_ADS_CONFIGURATION_FILE_PATH', 'Not set')
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
    logging.info('Processing Customer ID %s' % customer_id)
    ads_client = client.GoogleAdsClient.load_from_env(version=ADS_API_VERSION)
    ads_client.login_customer_id = login_customer_id
    # constants_gaql will be empty on subsequent invocations
    for resource, query, mode in accounts_gaql + [g for g in constants_gaql]:
      logging.info('...pulling resource %s...' % resource)
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
  # Preload Merchant Center authinfo so we know which accounts (for this
  # user) are top-level.
  # The sets of accessible aggregators and standlone accounts
  merchant_api = _get_merchant_center_api()
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
      for response in resource_downloader.download_resources(
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
        parent = {'settings': response}
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
        output_file = mc_path / aggregator_id / name / 'rows.jsonlines'

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open('w') as f:
          print(json.dumps(parent), file=f)

  # Process all non-aggregator account data in parallel
  with futures.ProcessPoolExecutor(
      mp_context=mp.get_context('spawn')
  ) as executor:
    future_results: dict[futures.Future[None], str] = {}
    for account_id in leaf_ids | (standalone_ids & input_ids):
      logging.info('Processing Merchant Center ID %s...' % account_id)
      # We need account-level resources
      parent_id = leaf_to_parent.get(account_id, account_id)
      for resource in acit_account_resources:
        # TODO(b/305301891): Remove after shippingsettings.list is fixed.
        if (
            account_id in standalone_ids
            or resource == _ACIT_MC_SHIPPINGSETTINGS_RESOURCE
        ):
          future = executor.submit(
              _pull_standalone_account_resource,
              str(mc_path),
              parent_id,
              account_id,
              resource,
          )
          future_results[future] = f'{parent_id}/{resource}/{account_id}'

      for resource in _ACIT_MC_RESOURCES:
        future = executor.submit(
            _pull_leaf_collection, str(mc_path), account_id, resource
        )
        future_results[future] = f'{parent_id}/{resource}/{account_id}'

    for completed in futures.as_completed(future_results):
      api_path = future_results[completed]
      try:
        completed.result()
        logging.info('Finished loading leaf resource: %s' % (api_path))
      except http.HttpError as ex:
        raise ValueError('Error retrieving resource %s' % (api_path)) from ex

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
