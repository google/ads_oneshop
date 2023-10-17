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
from absl.testing import parameterized

from acit import create_base_tables

import apache_beam as beam
from apache_beam.testing import test_pipeline
from apache_beam.testing import util


class BeamTest(parameterized.TestCase):

  @parameterized.named_parameters(
      {
          'testcase_name': 'catch_all',
          'tree': {},
          'path': [],
          'expected': {'isTargeted': True},
      },
      {
          'testcase_name': 'add_first_path_single',
          'tree': {},
          'path': [{'productItemId': {'value': 'abc123'}}],
          'expected': {
              'children': [{
                  'node': {'productItemId': {'value': 'abc123'}},
                  'isTargeted': True,
              }]
          },
      },
      {
          'testcase_name': 'add_first_path_multi',
          'tree': {},
          'path': [
              {'productType': {'value': 'product1', 'level': 'LEVEL1'}},
              {'productType': {'value': 'product2', 'level': 'LEVEL2'}},
              {'productCustomAttribute': {'index': 'INDEX0'}},
          ],
          'expected': {
              'children': [{
                  'node': {
                      'productType': {'value': 'product1', 'level': 'LEVEL1'}
                  },
                  'children': [{
                      'node': {
                          'productType': {
                              'value': 'product2',
                              'level': 'LEVEL2',
                          }
                      },
                      'children': [{
                          'node': {
                              'productCustomAttribute': {'index': 'INDEX0'}
                          },
                          'isTargeted': True,
                      }],
                  }],
              }]
          },
      },
      {
          'testcase_name': 'add_existing_tree',
          'tree': {
              'children': [{
                  'node': {'productItemId': {'value': 'abc123'}},
                  'isTargeted': False,
              }]
          },
          # Catch-all for "other product ids"
          'path': [{'productItemId': {}}],
          'expected': {
              'children': [
                  {
                      'node': {'productItemId': {'value': 'abc123'}},
                      'isTargeted': False,
                  },
                  {
                      'node': {'productItemId': {}},
                      'isTargeted': True,
                  },
              ]
          },
      },
      {
          'testcase_name': 'add_existing_tree_longer_path',
          'tree': {
              'children': [{
                  'node': {
                      'productType': {'value': 'product3', 'level': 'LEVEL1'}
                  },
                  'children': [],
              }]
          },
          'path': [
              {'productType': {'value': 'product1', 'level': 'LEVEL1'}},
              {'productType': {'value': 'product2', 'level': 'LEVEL2'}},
          ],
          'expected': {
              'children': [
                  {
                      'node': {
                          'productType': {
                              'value': 'product3',
                              'level': 'LEVEL1',
                          }
                      },
                      'children': [],
                  },
                  {
                      'node': {
                          'productType': {
                              'value': 'product1',
                              'level': 'LEVEL1',
                          }
                      },
                      'children': [{
                          'node': {
                              'productType': {
                                  'value': 'product2',
                                  'level': 'LEVEL2',
                              }
                          },
                          'isTargeted': True,
                      }],
                  },
              ]
          },
      },
      {
          'testcase_name': 'add_existing_tree_merge_child',
          'tree': {
              'children': [
                  {
                      'node': {
                          'productType': {
                              'value': 'product1',
                              'level': 'LEVEL1',
                          }
                      },
                      'children': [{
                          'node': {
                              'productType': {
                                  'value': 'product2',
                                  'level': 'LEVEL2',
                              }
                          },
                          'isTargeted': False,
                      }],
                  },
              ]
          },
          'path': [
              {'productType': {'value': 'product1', 'level': 'LEVEL1'}},
              {'productType': {'value': 'product3', 'level': 'LEVEL2'}},
          ],
          'expected': {
              'children': [
                  {
                      'node': {
                          'productType': {
                              'value': 'product1',
                              'level': 'LEVEL1',
                          }
                      },
                      'children': [
                          {
                              'node': {
                                  'productType': {
                                      'value': 'product2',
                                      'level': 'LEVEL2',
                                  }
                              },
                              'isTargeted': False,
                          },
                          {
                              'node': {
                                  'productType': {
                                      'value': 'product3',
                                      'level': 'LEVEL2',
                                  }
                              },
                              'isTargeted': True,
                          },
                      ],
                  },
              ]
          },
      },
  )
  def test_tree_creation(self, tree, path, expected):
    create_base_tables.build_tree(path, tree, is_targeted=True)
    self.assertTrue(
        expected.items() <= tree.items(),
        'Expected %s but got %s' % (expected, tree),
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'product_bidding_category_specific',
          'dimension': {
              'productBiddingCategory': {'id': '1', 'level': 'LEVEL1'}
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_bidding_category_wildcard',
          'dimension': {'productBiddingCategory': {'level': 'LEVEL1'}},
          'expected': True,
      },
      {
          'testcase_name': 'product_brand_specific',
          'dimension': {'productBrand': {'value': 'Some Brand'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_brand_wildcard',
          'dimension': {'productBrand': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_channel_specific',
          'dimension': {'productChannel': {'channel': 'Some Channel'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_channel_wildcard',
          'dimension': {'productChannel': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_condition_specific',
          'dimension': {'productCondition': {'condition': 'new'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_condition_wildcard',
          'dimension': {'productCondition': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_custom_attribute_specific',
          'dimension': {'productCustomAttribute': {'value': 'some attribute'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_custom_attribute_wildcard',
          'dimension': {'productCustomAttribute': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_item_specific',
          'dimension': {'productItemId': {'value': 'asdf'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_item_wildcard',
          'dimension': {'productItemId': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_type_specific',
          'dimension': {'productType': {'value': 'some type'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_type_wildcard',
          'dimension': {'productType': {}},
          'expected': True,
      },
  )
  def test_dimension_is_wildcard(self, dimension, expected):
    self.assertEqual(
        create_base_tables.dimension_is_wildcard(dimension),
        expected,
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'product_category_top_level_match',
          'product': {'googleProductCategory': 'Animals & Pet Supplies'},
          'dimension': {
              'productBiddingCategory': {'id': '1', 'level': 'LEVEL1'}
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_category_top_level_mismatch',
          'product': {'googleProductCategory': 'Animals & Pet Supplies'},
          'dimension': {
              'productBiddingCategory': {'id': '2', 'level': 'LEVEL1'}
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_category_top_level_wildcard_match',
          'product': {'googleProductCategory': 'Animals & Pet Supplies'},
          'dimension': {'productBiddingCategory': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_brand_match',
          'product': {'brand': 'Some Brand'},
          'dimension': {'productBrand': {'value': 'Some Brand'}},
          'expected': True,
      },
      {
          'testcase_name': 'product_channel_match',
          'product': {'channel': 'online'},
          'dimension': {'productChannel': {'channel': 'online'}},
          'expected': True,
      },
      {
          'testcase_name': 'product_condition_match',
          'product': {'condition': 'new'},
          'dimension': {'productCondition': {'condition': 'new'}},
          'expected': True,
      },
      # TODO: custom attribute out of index
      {
          'testcase_name': 'product_custom_attribute_match',
          'product': {
              'customLabel0': 'first attribute',
              'customLabel1': 'some attribute',
              'customLabel4': 'ignored',
          },
          'dimension': {
              # 0-indexed
              # Confusingly this refers to product labels, not attributes
              'productCustomAttribute': {
                  'value': 'some attribute',
                  'index': 'INDEX1',
              }
          },
          'expected': True,
      },
      {
          'testcase_name': 'product_item_match',
          'product': {'offerId': 'asdf'},
          'dimension': {'productItemId': {'value': 'asdf'}},
          'expected': True,
      },
      # TODO: Same test cases as above
      {
          'testcase_name': 'product_type_match',
          # Ads only uses the first product type, no matter what
          'product': {
              'productTypes': [
                  'First type > some type > Other type',
                  'ignored taxonomy',
                  'some bad > > > data',
              ]
          },
          'dimension': {
              # 1-indexed
              'productType': {'value': 'some type', 'level': 'LEVEL2'}
          },
          'expected': True,
      },
  )
  def test_specific_dimension_matches_product(
      self, product, dimension, expected
  ):
    self.assertEqual(
        create_base_tables.dimension_matches_product(product, dimension),
        expected,
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'catch_all',
          'product': {},
          'tree': {'isTargeted': True},
          'expected': True,
      },
      {
          'testcase_name': 'basic_path_match',
          'product': {'googleProductCategory': 'Animals & Pet Supplies'},
          'tree': {
              'children': [
                  {
                      'node': {
                          'productBiddingCategory': {
                              'level': 'LEVEL1',
                              'id': '1',
                          }
                      },
                      'isTargeted': True,
                  },
                  {
                      'node': {'productBiddingCategory': {}},
                      'isTargeted': False,
                  },
              ]
          },
          'expected': True,
      },
      {
          'testcase_name': 'basic_path_wildcard',
          'product': {'googleProductCategory': 'Animals & Pet Supplies'},
          'tree': {
              'children': [
                  {
                      'node': {
                          'productBiddingCategory': {
                              'level': 'LEVEL1',
                              'id': '2',
                          }
                      },
                      'isTargeted': False,
                  },
                  {
                      'node': {'productBiddingCategory': {}},
                      'isTargeted': True,
                  },
              ]
          },
          'expected': True,
      },
  )
  def test_product_targeted_by_tree(self, product, tree, expected):
    self.assertEqual(
        create_base_tables.product_targeted_by_tree(product, tree), expected
    )

  def test_cross_join_of_products_and_pmax(self):
    expected = [
        {
            'accountId': '098',
            'assetGroupId': 1000,
            'campaignId': 456,
            'customerId': 123,
            'isPMaxTargeted': True,
            'offerId': 'abc',
            'pmaxFilters': {
                'children': [{
                    'isTargeted': True,
                    'node': {'productItemId': 'abc'},
                }]
            },
            'product': {'offerId': 'abc'},
        },
        {
            'accountId': '098',
            'assetGroupId': 2000,
            'campaignId': 456,
            'customerId': 123,
            'isPMaxTargeted': False,
            'offerId': 'abc',
            'pmaxFilters': {
                'children': [{
                    'isTargeted': False,
                    'node': {'productItemId': 'xyz'},
                }]
            },
            'product': {'offerId': 'abc'},
        },
    ]

    product_statuses = [{
        'accountId': '098',
        'offerId': 'abc',
        'product': {'offerId': 'abc'},
    }]

    asset_group_filters = [
        {
            'customer': {'id': 123},
            'campaign': {'id': 456},
            'assetGroup': {'id': 1000},
            'assetGroupListingGroupFilter': {
                'type': 'UNIT_INCLUDED',
                'path': {'dimensions': [{'productItemId': 'abc'}]},
            },
        },
        {
            'customer': {'id': 123},
            'campaign': {'id': 456},
            'assetGroup': {'id': 2000},
            'assetGroupListingGroupFilter': {
                'type': 'UNIT_EXCLUDED',
                'path': {'dimensions': [{'productItemId': 'xyz'}]},
            },
        },
    ]

    with test_pipeline.Pipeline() as p:
      products = p | 'Create products' >> beam.Create(product_statuses)
      filters = p | 'Create filters' >> beam.Create(asset_group_filters)
      out = products | create_base_tables.PMaxTargeting(filters)

      util.assert_that(out, util.equal_to(expected))

  def test_deduplicate_pmax_asset_group_filter(self):
    expected = [
        {
            'customerId': 123,
            'accountId': '098',
            'offerId': 'abc',
            'isPMaxTargeted': True,
            # Provides traceability
            'targetingPMaxFilters': [{
                'campaignId': 456,
                'assetGroupId': 1000,
                'children': [{
                    'isTargeted': True,
                    'node': {'productItemId': 'abc'},
                }],
            }],
        },
    ]

    joined_pmax_filters = [
        {
            'accountId': '098',
            'assetGroupId': 1000,
            'campaignId': 456,
            'customerId': 123,
            'isPMaxTargeted': True,
            'offerId': 'abc',
            'pmaxFilters': {
                'children': [{
                    'isTargeted': True,
                    'node': {'productItemId': 'abc'},
                }]
            },
        },
        {
            'accountId': '098',
            'assetGroupId': 2000,
            'campaignId': 456,
            'customerId': 123,
            'isPMaxTargeted': False,
            'offerId': 'abc',
            'pmaxFilters': {
                'children': [{
                    'isTargeted': False,
                    'node': {'productItemId': 'xyz'},
                }]
            },
        },
    ]

    with test_pipeline.Pipeline() as p:
      out = (
          p
          | 'Create filters' >> beam.Create(joined_pmax_filters)
          | create_base_tables.DeduplicatePMaxTargeting()
      )

      util.assert_that(out, util.equal_to(expected))

  def test_pipeline_reports_products_without_pmax_campaigns(self):
    expected = [{'product': {'offerId': 'abc'}}]
    with test_pipeline.Pipeline() as p:
      products = p | 'Products' >> beam.Create(expected)
      filters = p | 'Empty filters' >> beam.Create([])
      out = products | create_base_tables.PMaxTargeting(filters)
      util.assert_that(out, util.equal_to(expected))
