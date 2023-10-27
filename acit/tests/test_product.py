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

from acit import product as product_util

_PRODUCT_CATEGORIES_BY_ID = {
    '1': 'Animals & Pet Supplies',
    '2': 'Other',
}


class ProductTest(parameterized.TestCase):

  @parameterized.named_parameters(
      {
          'testcase_name': 'product_bidding_category_wildcard',
          'dimension': {'productBiddingCategory': {'level': 'LEVEL5'}},
          'expected': True,
      },
      {
          'testcase_name': 'product_bidding_category_specific',
          'dimension': {
              'productBiddingCategory': {'id': '1', 'level': 'LEVEL5'}
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_brand_wildcard',
          'dimension': {'productBrand': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_brand_specific',
          'dimension': {'productBrand': {'value': 'Brand Name'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_channel_wildcard',
          'dimension': {'productChannel': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_channel_specific',
          'dimension': {'productChannel': {'channel': 'ONLINE'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_channel_exclusivity_wildcard',
          'dimension': {'productChannelExclusivity': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_channel_exclusivity_specific',
          'dimension': {
              'productChannelExclusivity': {
                  'channelExclusivity': 'MULTI_CHANNEL'
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_condition_wildcard',
          'dimension': {'productCondition': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_condition_specific',
          'dimension': {'productCondition': {'condition': 'NEW'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_custom_attribute_wildcard',
          'dimension': {'productCustomAttribute': {'index': 'INDEX0'}},
          'expected': True,
      },
      {
          'testcase_name': 'product_custom_attribute_specific',
          'dimension': {
              'productCustomAttribute': {
                  'index': 'INDEX4',
                  'value': 'Some Custom Attribute',
              }
          },
          'expected': False,
      },
      {
          'testcase_name': 'product_item_id_wildcard',
          'dimension': {'productItemId': {}},
          'expected': True,
      },
      {
          'testcase_name': 'product_item_id_specific',
          'dimension': {'productItemId': {'value': 'my_product_id'}},
          'expected': False,
      },
      {
          'testcase_name': 'product_type_wildcard',
          'dimension': {'productType': {'level': 'LEVEL1'}},
          'expected': True,
      },
      {
          'testcase_name': 'product_type_specific',
          'dimension': {
              'productType': {'level': 'LEVEL5', 'value': 'My product type'}
          },
          'expected': False,
      },
  )
  def test_dimension_is_wildcard(self, dimension, expected):
    result = product_util.dimension_is_wildcard(dimension)
    self.assertEqual(result, expected)

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
        product_util.dimension_matches_product(
            product, dimension, _PRODUCT_CATEGORIES_BY_ID
        ),
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
                      'dimension': {
                          'productBiddingCategory': {
                              'level': 'LEVEL1',
                              'id': '1',
                          }
                      },
                      'isTargeted': True,
                  },
                  {
                      'dimension': {'productBiddingCategory': {}},
                      'isTargeted': False,
                  },
              ],
              'dimension': {},
          },
          'expected': True,
      },
      {
          'testcase_name': 'basic_path_wildcard',
          'product': {'googleProductCategory': 'Animals & Pet Supplies'},
          'tree': {
              'children': [
                  {
                      'dimension': {
                          'productBiddingCategory': {
                              'level': 'LEVEL1',
                              'id': '2',
                          }
                      },
                      'isTargeted': False,
                  },
                  {
                      'dimension': {'productBiddingCategory': {}},
                      'isTargeted': True,
                  },
              ],
              'dimension': {},
          },
          'expected': True,
      },
  )
  def test_product_targeted_by_tree(self, product, tree, expected):
    self.assertEqual(
        product_util.product_targeted_by_tree(
            product, tree, _PRODUCT_CATEGORIES_BY_ID
        ),
        expected,
    )
