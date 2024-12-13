# Copyright 2024 Google LLC
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

from google.ads.googleads.v17.services.types import google_ads_service

from acit import shopping


class ShoppingTest(parameterized.TestCase):

  def test_build_product_group_tree_single_leaf(self):
    ad_group_criterion = google_ads_service.GoogleAdsRow(
        {
            'customer': {
                'id': 123,
            },
            'campaign': {
                'id': 456,
            },
            'ad_group': {
                'id': 789,
            },
            'ad_group_criterion': {
                'negative': False,
                'listing_group': {'path': None},
            },
        }
    )
    expected = {
        'customer_id': '123',
        'campaign_id': '456',
        'tree_parent_id': '789',
        'node': {'children': [], 'dimension': {}, 'isTargeted': True},
    }
    criteria = [
        google_ads_service.GoogleAdsRow.to_dict(
            ad_group_criterion, preserving_proto_field_name=False
        )
    ]
    _, actual = shopping.build_product_group_tree(None, criteria)

    self.assertEqual(expected, actual)

  def test_build_product_group_tree_unbalanced_binary_tree(self):
    """Tests bad data scenario with ad group listing group criteria."""

    ad_group_criterion = google_ads_service.GoogleAdsRow(
        {
            'customer': {
                'id': 123,
            },
            'campaign': {
                'id': 456,
            },
            'ad_group': {
                'id': 789,
            },
            'ad_group_criterion': {
                'negative': False,
                'listing_group': {
                    'path': {
                        'dimensions': [
                            {
                                'product_type': {
                                    'level': 'LEVEL1',
                                    'value': 'my level1 type',
                                },
                            }
                        ],
                    }
                },
            },
        }
    )
    criteria = [
        google_ads_service.GoogleAdsRow.to_dict(
            ad_group_criterion,
            use_integers_for_enums=False,
            preserving_proto_field_name=False,
        )
    ]
    expected = {
        'customer_id': '123',
        'campaign_id': '456',
        'tree_parent_id': '789',
        'node': {
            'children': [
                {
                    'children': [],
                    'dimension': {
                        'productType': {
                            'level': 'LEVEL1',
                            'value': 'my level1 type',
                        }
                    },
                    'isTargeted': True,
                }
            ],
            'dimension': {},
            'isTargeted': None,
        },
    }
    _, actual = shopping.build_product_group_tree(None, criteria)

    self.assertEqual(expected, actual)
