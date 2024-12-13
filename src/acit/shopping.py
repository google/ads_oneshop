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
"""Tools for identifying Product relations to Shopping campaigns."""

# Omitting type hints for Google Ads, as this causes severe memory bloat,
#   is unavailable internally, and we're using JSON-parsed objects here.

import apache_beam as beam

from typing import TypedDict, Any, Tuple
from collections.abc import Iterable

from acit import product as util


class CampaignInventoryFilter(TypedDict):
  dimensions: list[Any]


class Campaign(TypedDict):
  type_: str
  customer_id: str
  campaign_id: str
  merchant_center_id: str
  feed_label: str
  enable_local: bool
  scope: CampaignInventoryFilter
  targeting: list[util.ProductTargetingTree]


def _extract_customer_campaign_ids(row):
  return (
      row['customer']['id'],
      row['campaign']['id'],
  )


CustomerId = str
CampaignId = str
CampaignIds = Tuple[CustomerId, CampaignId]


def build_product_group_tree(
    keys, criteria
) -> Tuple[CampaignIds, util.ProductTargetingTree]:
  assert criteria
  first, rest = criteria[0], criteria[1:]
  customer_id = first['customer']['id']
  campaign_id = first['campaign']['id']
  ad_group_id = first['adGroup']['id']
  node = util.ProductTargetingNode(children=[], dimension={}, isTargeted=None)
  path = first['adGroupCriterion']['listingGroup'].get('path', {})
  if not rest and not path:
    # only one LISTING_GROUP criterion: this must be a wildcard
    node['isTargeted'] = not first['adGroupCriterion']['negative']
  else:
    for criterion in [first] + rest:
      is_targeted = not criterion['adGroupCriterion']['negative']
      util.build_product_group_tree(path['dimensions'], node, is_targeted)

  tree = util.ProductTargetingTree(
      customer_id=customer_id,
      campaign_id=campaign_id,
      tree_parent_id=ad_group_id,
      node=node,
  )
  return keys, tree


class _CoGroupByKeyShoppingCampaign(TypedDict):
  campaigns: list[Any]
  campaign_criteria: list[Any]
  product_group_trees: list[util.ProductTargetingTree]


def _build_campaign_inventory_filter(
    campaign_criteria, tree: CampaignInventoryFilter
):
  if not campaign_criteria:
    return
  assert len(campaign_criteria) == 1
  criterion = campaign_criteria[0]
  tree['dimensions'] = criterion['campaignCriterion']['listingScope'][
      'dimensions'
  ]


def create_shopping_campaign(
    unused_keys: CampaignIds, grouping: _CoGroupByKeyShoppingCampaign
) -> Campaign:
  campaigns = grouping['campaigns']
  assert campaigns
  for campaign in grouping['campaigns']:
    type_: str = campaign['campaign']['advertisingChannelType']
    customer_id: str = campaign['customer']['id']
    campaign_id: str = campaign['campaign']['id']
    shopping_settings = campaign['campaign'].get('shoppingSetting', {})
    merchant_id: str = shopping_settings.get('merchantId', '')
    feed_label: str = shopping_settings.get('feedLabel', '')
    enable_local: bool = shopping_settings.get('enableLocal', False)

    scope = CampaignInventoryFilter(dimensions=[])
    result = Campaign(
        type_=type_,
        customer_id=customer_id,
        campaign_id=campaign_id,
        merchant_center_id=merchant_id,
        feed_label=feed_label,
        enable_local=enable_local,
        scope=scope,
        targeting=grouping['product_group_trees'],
    )
    criteria = grouping['campaign_criteria']
    _build_campaign_inventory_filter(criteria, scope)
    return result
  assert False
