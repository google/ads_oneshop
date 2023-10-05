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
  )
  def test_dimension_matches_product(self, product, dimension, expected):
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
