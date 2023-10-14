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
from absl import app

from typing import Dict, Any

import itertools
import json

from acit import resource_downloader
from acit import product_category

import apache_beam as beam
from apache_beam.io import textio


def ReadGoogleAdsRows(description: str, path: str) -> beam.ParDo:
  return textio.ReadFromText(
      path
  ) | f'{description} to Google Ads Row' >> beam.Map(json.loads)


@beam.ptransform_fn
def AnnotateProducts(products):
  def in_stock(p):
    p['inStock'] = 'in stock' == p['product']['availability']
    return p

  def approved(p):
    statuses = [s['status'] for s in p['status']['destinationStatuses']]
    approved = 'approved' in statuses
    pending = 'pending' in statuses
    disapproved = 'disapproved' in statuses
    # If any limitation appears, flag it
    p['approved'] = approved and not pending and not disapproved
    return p

  return (
      products
      | 'Approved' >> beam.Map(approved)
      | 'In Stock' >> beam.Map(in_stock)
  )


@beam.ptransform_fn
def JoinProductStatuses(
    products: beam.PCollection, statuses: beam.PCollection
) -> beam.PTransform:
  def product(kv):
    k, v = kv
    merchant_id, offer_id = k
    # This should always be 1:1
    for t in itertools.product(v['products'], v['statuses']):
      yield {
          'accountId': merchant_id,
          'offerId': offer_id,
          'product': t[0],
          'status': t[1],
      }

  return (
      {
          'products': products
          | 'Prep products for join'
          >> beam.Map(
              lambda p: (
                  (
                      p[resource_downloader.METADATA_KEY]['accountId'],
                      p['id'],
                  ),
                  p,
              )
          ),
          'statuses': statuses
          | 'Prep product statuses for join'
          >> beam.Map(
              lambda s: (
                  (
                      s[resource_downloader.METADATA_KEY]['accountId'],
                      s['productId'],
                  ),
                  s,
              )
          ),
      }
      | 'Group product tables' >> beam.CoGroupByKey()
      | 'Join product tables' >> beam.FlatMap(product)
  )


# TODO: Remove
def listing_group_trees(filters):
  """
  cross-join all products against all leaf filters
    groupby product id, campaign id, asset group id: [modded leaf filters]
    map: filter applicable rules; retain max length paths; prefer valued
      return single unit include/exclude type and criteria ID
    group by product id: [each campaign's asset group's filter group full path]
    # skip group by campaign since campaign:asset_group is often 1:1
    map: whether any rule exists,
         across all campaigns,
         where product is included and not top-level everything
  """
  pass


@beam.ptransform_fn
def AnnotateListingGroupFilters(filters):
  """
  Group by Asset Group. For each asset group, take all leaf filters,
  get the length of all matching leaf filters, and whether they're
  'everything else'
  """
  filters | 'Filter Length' >> beam.Map(
      lambda f: len(f['assetGroupListingGroupFilter']['path']['dimensions'])
  )
  raise NotImplementedError


@beam.ptransform_fn
def MatchFilters(products, filters):
  """For each product, find matching listing group filters per asset group."""
  raise NotImplementedError


def dimension_is_wildcard(d):
  if 'productBiddingCategory' in d:
    return 'id' not in d['productBiddingCategory']
  if 'productItemId' in d:
    return 'value' not in d['productItemId']
  # TODO: Implement other dimensions


_PRODUCT_CATEGORY_LEVELS = [
    'LEVEL1',
    'LEVEL2',
    'LEVEL3',
    'LEVEL4',
    'LEVEL5',
]


def dimension_matches_product(p, d):
  # TODO: Implement other dimensions
  if 'productBiddingCategory' in d:
    info = d['productBiddingCategory']
    # Wildcard
    if 'id' not in info or 'level' not in info:
      return True
    level = _PRODUCT_CATEGORY_LEVELS.index(info['level'])
    google_product_category: str = p.get('googleProductCategory', '')
    tokens = google_product_category.split(' > ')
    if level >= len(tokens):
      # Ad criteria is too granular
      return False
    google_product_category = ' > '.join(tokens[: level + 1])
    # If there is no ID (wildcard), return True
    return int(info['id']) == product_category.id_by_path(
        google_product_category
    )
  if 'productItemId' in d:
    # Handle wildcards
    match = p['offerId']
    return d.get('value', match) == d['productItemId']
  return False


def product_targeted_by_tree(p, t: Dict[str, Any]):
  if 'isTargeted' in t:
    return t['isTargeted']
  real_match = None
  catch_all = None
  for c in t['children']:
    # There can only be 2 possibilities
    if dimension_is_wildcard(c['node']):
      catch_all = c
    elif dimension_matches_product(p, c['node']):
      real_match = c
  return product_targeted_by_tree(p, real_match or catch_all)


def annotate_pmax_filter(product_with_filters):
  p = product_with_filters['product']
  tree = product_with_filters['pmaxFilters']
  product_with_filters['isPMaxTargeted'] = product_targeted_by_tree(p, tree)
  return product_with_filters


def join_filters_to_products(kv):
  # TODO: K should shard by customer ID, Date
  k, v = kv
  products = v['products']
  for tree in v['pmax_filters']:
    # TODO: Figure out how to "trace back" a given campaign, asset group, and
    #       filter to the serving of a given product.
    asset_group_id, filters = tree
    for p in products:
      p['pmaxFilters'] = filters
      yield p


def _build_tree(path, t, is_targeted):
  """
  Recursively builds a tree
  Args:
    path: A list of dimensions to a leaf in an asset group listing group filter
    t: The parent tree with children to add this path to
    is_targeted: Whether the leaf at the end of this path should be included.
  """
  if not path:
    return
  # Check if there's an existing child (this path shares common ancestor(s))
  child = next(iter([c for c in t['children'] if c['node'] == path[0]]), None)
  if not child:
    # Is this a leaf?
    if len(path) == 1:
      child = {'node': path[0], 'isTargeted': is_targeted}
      t['children'].append(child)
      return
    else:
      child = {'node': path[0], 'children': []}
      t['children'].append(child)
      _build_tree(path[1:], child, is_targeted)
  else:
    _build_tree(path[1:], child, is_targeted)


def build_tree(path, t: Dict[str, Any], is_targeted):
  if not path:
    # Case when root node is a catch-all
    t['isTargeted'] = is_targeted
    return
  if not t:
    # no prior path has built the root node
    t['children'] = []
  _build_tree(path, t, is_targeted)


def build_listing_group_tree(asset_group_id, filters):
  t = {}
  if not filters:
    return (asset_group_id, t)

  for filter in filters:
    f = filter['assetGroupListingGroupFilter']
    path = f['path'].get('dimensions', [])
    is_targeted = 'UNIT_INCLUDED' == f['type']
    build_tree(path, t, is_targeted)

  return (asset_group_id, t)


@beam.ptransform_fn
def PMaxTargeting(products, filters):
  """Determines whether a product is targeted by a PMax campaign.

  'Targeted' means that there exists a 'UNIT_INCLUDE' rule in any PMax campaign
  asset group matching the product's targeting dimensions, and that if that rule
  is a catch-all, the product is not matched by a 'UNIT_EXCLUDE' rule.
  """

  all_asset_group_filters = (
      filters
      | 'Group filters by asset group'
      >> beam.GroupBy(lambda f: f['assetGroup']['id'])
      | beam.MapTuple(build_listing_group_tree)
      # Force all asset groups to a single value.
      # There can be 10,000 campaigns per CID, 100 asset groups campaign, and
      # 1,000 listing group filters per asset group: 1 Billion filters per CID.
      # Assume ~300 bytes per filter JSON object.
      # Implies 300 GB max memory per CID.
      #
      # TODO: update this by customer ID and date
      | 'Prepare for Cross Join' >> beam.Map(lambda f: (1, f))
  )

  # TODO: map products by date and customer ID
  # TODO: inject in Customer ID data from merchants
  # TODO: strip objects down and use protobuf for memory savings
  # TODO: figure out a more efficient cross-join, if possible
  all_products = products | beam.Map(lambda p: (1, p))

  targeted_products = (
      {'products': all_products, 'pmax_filters': all_asset_group_filters}
      | 'apply pmax filters to products' >> beam.CoGroupByKey()
      | beam.FlatMap(join_filters_to_products)
      | beam.Map(annotate_pmax_filter)
  )

  return targeted_products

  return (
      products | MatchFilters(all_asset_group_filters) | annotate_pmax_filter()
  )


# Flags after `--` can get passed directly
def main(argv):
  with beam.Pipeline() as p:
    # Ads Data
    asset_group_listing_filters = (
        p
        | 'Read Asset Group Listing Filters'
        >> ReadGoogleAdsRows(
            'Asset Group Listing Filters',
            '/tmp/acit/*/ads/*/asset_group_listing_filter/*.jsonlines',
        )
    )
    shopping_performance_views = (
        p
        | 'Read Shopping Performance Views'
        >> ReadGoogleAdsRows(
            'Shopping Performance View',
            '/tmp/acit/*/ads/*/shopping_performance_view/*.jsonlines',
        )
    )

    # Merchant Center data
    # TODO: account-level data
    products = (
        p
        | 'Read Products'
        >> textio.ReadFromText(
            '/tmp/acit/*/merchant_center/*/products/*.jsonlines'
        )
        | 'Products to JSON' >> beam.Map(json.loads)
    )
    product_statuses = (
        p
        | 'Read Product Statuses'
        >> textio.ReadFromText(
            '/tmp/acit/*/merchant_center/*/productstatuses/*.jsonlines'
        )
        | 'Product Statuses to JSON' >> beam.Map(json.loads)
    )

    all_products = (
        products
        | JoinProductStatuses(product_statuses)
        | AnnotateProducts()
        | 'Calculate PMax Targeting'
        >> PMaxTargeting(asset_group_listing_filters)
    )

    # TODO: Is this true?
    # As it turns out, impressions are a prereq for the shopping_performance_view
    # So we can't even tell if something is targeted if it doesn't show up

    all_products | 'JSON' >> beam.Map(json.dumps) | 'Print' >> beam.Map(print)


if __name__ == '__main__':
  app.run(main)
