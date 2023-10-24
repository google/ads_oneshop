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
"""Tools for identifying Product relations to Performance Max campaigns."""

# Omitting type hints for Google Ads, as this causes severe memory bloat,
#   is unavailable internally, and we're using JSON-parsed objects here.


from typing import Dict, List, Iterable, Any, Tuple

from acit import product as util


def build_product_targeting_tree(
    keys, criteria: List[Any]
) -> Tuple[Any, util.ProductTargetingTree]:
  """Creates PMax targeting tree from Asset Group Listing Group Filters.

  Recursive. Google Ads limits depth to a single-digit number.

  Args:
    keys: The keys to return. Used in Beam for KV tables.
    criteria: The Google Ads Row object containing the Asset Group Listing Filter

  Returns:
    An iterable containing the generated tree, as required by the Beam API.
  """
  assert criteria
  first, rest = criteria[0], criteria[1:]
  customer_id = first['customer']['id']
  campaign_id = first['campaign']['id']
  asset_group_id = first['assetGroup']['id']

  node = util.ProductTargetingNode(children=[], dimension={}, isTargeted=None)
  asset_group_filter = first['assetGroupListingGroupFilter']
  is_targeted = 'UNIT_INCLUDED' == asset_group_filter['type']
  if not rest:
    # only one listing group criterion: this must be a wildcard
    assert not asset_group_filter['path'].get('dimensions', [])
    is_targeted = 'UNIT_INCLUDED' == asset_group_filter['type']
    node['isTargeted'] = is_targeted
  else:
    for criterion in [first] + rest:
      asset_group_filter = criterion['assetGroupListingGroupFilter']
      dimensions = asset_group_filter['path'].get('dimensions', [])
      is_targeted = 'UNIT_INCLUDED' == asset_group_filter['type']
      util.build_product_group_tree(dimensions, node, is_targeted)

  tree = util.ProductTargetingTree(
      customer_id=customer_id,
      campaign_id=campaign_id,
      tree_parent_id=asset_group_id,
      node=node,
  )
  return keys, tree
