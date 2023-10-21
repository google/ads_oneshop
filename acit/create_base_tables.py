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
"""Create base table files for Merchant Center products in Google Ads.

Aggregates the following data:

 - Merchant Center:
   - Account-level data, such as:
     - Links to Google Ads
     - Shipping settings
     - Local inventory settings
   - Products and their statuses within Merchant Center
 - Google Ads
   - All targeting settings for campaigns driven by Merchant Center feeds
   - The advertising performance of those products
"""

from absl import app
from absl import flags

import json

from acit import merchant_account
from acit import performance_max
from acit import product
from acit import resource_downloader

import apache_beam as beam
from apache_beam.io import textio
from apache_beam.options import pipeline_options
from apache_beam import pvalue

# Omit variable declaration so we can pickle __main__.
flags.DEFINE_string('output', 'out.jsonlines', 'The path to output to')


def _ReadGoogleAdsRows(description: str, path: str) -> beam.ParDo:
  """Simple textio wrapper, can be used to swap in Ads protos later."""
  return textio.ReadFromText(
      path
  ) | f'{description} to Google Ads Row' >> beam.Map(json.loads)


# Flags after `--` can get passed directly to Beam
def main(argv):
  opts = pipeline_options.PipelineOptions(argv[1:])
  opts.view_as(pipeline_options.SetupOptions).save_main_session = True

  # Pre-load objects in main before pipeline creation
  category_cache = product.ProductCategoryCache()
  pmax_matcher = performance_max.TargetingMatcher(category_cache=category_cache)

  with beam.Pipeline(options=opts) as p:
    # Ads Data
    asset_group_listing_filters = (
        p
        | 'Read Asset Group Listing Filters'
        >> _ReadGoogleAdsRows(
            'Asset Group Listing Filters',
            '/tmp/acit/*/ads/*/asset_group_listing_filter/*.jsonlines',
        )
    )
    shopping_performance_views = (
        p
        | 'Read Shopping Performance Views'
        >> _ReadGoogleAdsRows(
            'Shopping Performance View',
            '/tmp/acit/*/ads/*/shopping_performance_view/*.jsonlines',
        )
    )

    # Merchant Center data
    # TODO: other account-level data
    accounts = (
        p
        | 'Read Accounts'
        >> textio.ReadFromText(
            '/tmp/acit/*/merchant_center/*/accounts/*.jsonlines'
        )
        | 'Accounts to JSON' >> beam.Map(json.loads)
    )
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

    # Precompute PMax targeting sideinput
    pmax_trees_by_cid = (
        asset_group_listing_filters
        | 'Group filters by asset group'
        >> beam.GroupBy(
            lambda f: (
                f['customer']['id'],
                f['campaign']['id'],
                f['assetGroup']['id'],
            )
        )
        # TODO: evaluate memory bloat by passing a fully-constructed object here.
        | beam.MapTuple(
            lambda ids, filters: (
                ids[0],
                pmax_matcher.build_listing_group_tree(filters),
            )
        )
        # Force all asset groups to a single CID.
        # There can be 10,000 campaigns per CID, 100 asset groups campaign, and
        # 1,000 listing group filters per asset group: 1 Billion filters per CID.
        # Assume ~300 bytes per filter JSON object.
        # Implies 300 GB max memory per CID. In practice, this is much lower.
        # Well-below GCE M2-tier limits.
        | 'Group by Customer ID' >> beam.GroupByKey()
    )

    # Precompute effective Merchant Center to Google Ads leaf links
    cids_by_merchant_id = (
        accounts
        | 'Get linked ads accounts'
        >> beam.FlatMap(merchant_account.extract_ads_ids_by_merchant)
        | 'Group by leaf Merchant ID' >> beam.GroupByKey()
    )

    # First, join all products and their 1:1 statuses
    product_statuses = (
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
            'statuses': product_statuses
            | 'Prep product statuses for join'
            >> beam.Map(
                lambda p: (
                    (
                        p[resource_downloader.METADATA_KEY]['accountId'],
                        p['productId'],
                    ),
                    p,
                )
            ),
        }
        | 'Group product tables' >> beam.CoGroupByKey()
        | 'Join product tables'
        >> beam.FlatMapTuple(product.inner_join_product_and_status)
    )

    all_products = (
        product_statuses
        # Extract info in the products themselves
        | 'Approved' >> beam.Map(product.set_product_approved)
        | 'In Stock' >> beam.Map(product.set_product_in_stock)
        # Add PMax targeting. Side-input views provide in-memory lookups.
        | 'Get PMax targeting'
        >> beam.FlatMap(
            pmax_matcher.get_pmax_targeting,
            pvalue.AsDict(cids_by_merchant_id),
            pvalue.AsDict(pmax_trees_by_cid),
        )
        | 'Combine PMax targeting'
        >> beam.MapTuple(
            lambda product, trees: {
                'productStatus': product,
                'hasPMaxTargeting': True if trees else False,
                'pMaxFilters': trees,
            }
        )
    )

    _ = (
        all_products
        | 'JSON' >> beam.Map(json.dumps)
        | textio.WriteToText(flags.FLAGS.output)
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  app.run(main)
