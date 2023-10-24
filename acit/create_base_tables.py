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

from acit import performance_max
from acit import shopping
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

    campaign_settings = p | 'Read Campaign Settings' >> _ReadGoogleAdsRows(
        'Campaign Settings',
        '/tmp/acit/*/ads/*/campaign/*.jsonlines',
    )

    campaign_criteria = p | 'Read Campaign Criteria' >> _ReadGoogleAdsRows(
        'Campaign Criteria',
        '/tmp/acit/*/ads/*/campaign_criterion/*.jsonlines',
    )

    ad_group_criteria = p | 'Read Ad Group Criteria' >> _ReadGoogleAdsRows(
        'Ad Group Criteria',
        '/tmp/acit/*/ads/*/ad_group_criterion/*.jsonlines',
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

    languages_by_campaign_id = (
        campaign_criteria
        | beam.Filter(lambda c: c['campaignCriterion']['type'] == 'LANGUAGE')
        | beam.Map(
            lambda c: (
                c['campaign']['id'],
                {
                    'language': c['campaignCriterion']['language'][
                        'languageConstant'
                    ].lower(),
                    'is_targeted': not c['campaignCriterion']['negative'],
                },
            )
        )
    )

    listing_scopes_by_campaign_id = (
        campaign_criteria
        | beam.Filter(
            lambda c: c['campaignCriterion']['type'] == 'LISTING_SCOPE'
        )
        | beam.Map(
            lambda c: (
                c['campaign']['id'],
                c['campaignCriterion']['listingScope']['dimensions'],
            )
        )
    )

    campaigns = (
        {
            'campaigns': campaign_settings
            | beam.Map(lambda c: (c['campaign']['id'], c)),
            'languages': languages_by_campaign_id,
            'inventory_filter_dimensions': listing_scopes_by_campaign_id,
        }
        | beam.CoGroupByKey()
        | beam.FlatMapTuple(
            lambda _, v: product.build_campaign(
                # Must only be one
                v['campaigns'][0],
                # Array
                v['languages'],
                # At-most one
                next(iter(v['inventory_filter_dimensions']), []),
            )
        )
    )

    shopping_campaigns_by_merchant_id = (
        campaigns
        | beam.Filter(lambda c: c['campaign_type'] == 'SHOPPING')
        | 'Group Shopping campaigns by Merchant ID'
        >> beam.GroupBy(lambda c: c['merchant_id'])
    )

    # Precompute shopping targeting sideinput
    shopping_trees_by_campaign_id = (
        ad_group_criteria
        | 'Group filters by ad group'
        >> beam.GroupBy(
            lambda f: (
                f['campaign']['id'],
                f['adGroup']['id'],
            )
        )
        | beam.MapTuple(shopping.build_product_group_tree)
        | beam.MapTuple(lambda ids, tree: (ids[0], tree))
        | 'Group Shopping Listing Group Trees by Campaign ID'
        >> beam.GroupByKey()
    )

    pmax_campaigns_by_merchant_id = (
        campaigns
        | beam.Filter(lambda c: c['campaign_type'] == 'PERFORMANCE_MAX')
        | 'Group PMax campaigns by Merchant ID'
        >> beam.GroupBy(lambda c: c['merchant_id'])
    )

    # Precompute PMax targeting sideinput
    pmax_trees_by_campaign_id = (
        asset_group_listing_filters
        | 'Group filters by asset group'
        >> beam.GroupBy(
            lambda f: (
                f['campaign']['id'],
                f['assetGroup']['id'],
            )
        )
        | beam.MapTuple(performance_max.build_product_targeting_tree)
        | beam.MapTuple(lambda ids, tree: (ids[0], tree))
        | 'Group PMax Listing Group Trees by Campaign ID' >> beam.GroupByKey()
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
            product.get_campaign_targeting,
            pvalue.AsDict(pmax_trees_by_campaign_id),
            pvalue.AsDict(pmax_campaigns_by_merchant_id),
        )
        | 'Combine PMax targeting'
        >> beam.MapTuple(
            lambda product, trees: {
                **product,
                'hasPMaxTargeting': True if trees else False,
                'pMaxFilters': trees,
            }
        )
        # Add Shopping targeting. Side-input views provide in-memory lookups.
        | 'Get Shopping targeting'
        >> beam.FlatMap(
            product.get_campaign_targeting,
            pvalue.AsDict(shopping_trees_by_campaign_id),
            pvalue.AsDict(shopping_campaigns_by_merchant_id),
        )
        | 'Combine Shopping targeting'
        >> beam.MapTuple(
            lambda product, trees: {
                **product,
                'hasShoppingTargeting': True if trees else False,
                'shoppingFilters': trees,
            }
        )
    )

    def products_table_row(row):
      # TODO: use this for the final products table schema
      return row

    _ = (
        all_products
        | 'Convert to final products table output'
        >> beam.Map(products_table_row)
        | 'JSON' >> beam.Map(json.dumps)
        | textio.WriteToText(flags.FLAGS.output)
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  app.run(main)
