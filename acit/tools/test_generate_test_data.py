from acit.tools import generate_test_data as gtd

import unittest
import random
import math
from absl.testing import absltest

from google.ads.googleads.v17.services.types import google_ads_service


class TestGenerator(absltest.TestCase):

  # Model generation
  def test_generate_model_number_of_products(self):
    g = gtd.Generator()
    spec = g.spec()
    expected_products = 2
    expected_online = expected_products // 2
    expected_local = expected_products // 2
    spec['products']['count'] = expected_products
    g.generate_model()

    model = g.model()
    n_products = 0
    n_online = 0
    n_local = 0
    for merchant in model['merchants']:
      for product in merchant['products']:
        n_products += 1
        if product['channel'] == 'online':
          n_online += 1
        if product['channel'] == 'local':
          n_local += 1

    self.assertTrue(expected_products, n_products)
    self.assertTrue(expected_local, n_local)
    self.assertTrue(expected_online, n_online)

  def test_generate_model_number_of_customers(self):
    g = gtd.Generator()
    spec = g.spec()
    expected_customers = 3
    spec['customers']['count'] = expected_customers
    g.generate_model()

    model = g.model()

    n_customers = sum([1 for _ in model['advertisers']])

    self.assertEqual(expected_customers, n_customers)

  def test_generate_model_split_of_campaign_types(self):
    g = gtd.Generator()
    spec = g.spec()
    expected_campaigns = 100
    expected_pmax = 33
    expected_shopping = 67
    spec['customers']['campaigns']['count'] = expected_campaigns
    spec['customers']['campaigns']['pmax']['percent'] = expected_pmax
    g.generate_model()

    model = g.model()

    n_campaigns = 0
    n_pmax = 0
    n_shopping = 0
    for customer in model['advertisers']:
      for campaign in customer['campaigns']:
        n_campaigns += 1
        if campaign['type'] == 'PERFORMANCE_MAX':
          n_pmax += 1
        if campaign['type'] == 'SHOPPING':
          n_shopping += 1

    self.assertTrue(expected_campaigns, n_campaigns)
    self.assertTrue(n_pmax < 50)
    self.assertTrue(50 < n_shopping)

  # Resource generation, primarily to check

  def test_create_products(self):
    g = gtd.Generator()
    spec = g.spec()
    expected_products = 100
    spec['products']['count'] = expected_products
    g.generate_model()

    actual = len(list(g.products()))
    self.assertEqual(expected_products, actual)

  def test_create_product_statuses(self):
    g = gtd.Generator()
    spec = g.spec()
    expected_product_statuses = 100
    spec['products']['count'] = expected_product_statuses
    g.generate_model()

    actual = len(list(g.product_statuses()))
    self.assertEqual(expected_product_statuses, actual)

  def test_create_campaigns(self):
    g = gtd.Generator()
    spec = g.spec()
    expected_campaigns = 100
    spec['customers']['campaigns']['count'] = expected_campaigns
    g.generate_model()

    actual = len(list(g.campaigns()))
    self.assertEqual(expected_campaigns, actual)

  def test_create_campaign_criteria_pmax(self):
    g = gtd.Generator()
    spec = g.spec()
    expected_campaign_criteria = 100
    spec['customers']['campaigns']['count'] = expected_campaign_criteria
    # We expect listing scope filters on some shopping campaigns
    spec['customers']['campaigns']['pmax']['percent'] = 100
    g.generate_model()

    actual = len(list(g.campaign_criteria()))
    self.assertEqual(
        expected_campaign_criteria,
        actual,
        'PMax campaigns should have exactly one campaign criterion, '
        'but more were found',
    )

  def test_create_campaign_criteria_shopping(self):
    g = gtd.Generator()
    spec = g.spec()
    n_campaigns = 100
    spec['customers']['campaigns']['count'] = n_campaigns
    spec['customers']['campaigns']['pmax']['percent'] = 0
    g.generate_model()

    n_criteria = len(list(g.campaign_criteria()))
    self.assertTrue(
        n_campaigns < n_criteria,
        'Some shopping campaigns should have additional criteria '
        'for listing scope filters',
    )

  def test_create_ad_group_criteria_pmax(self):
    g = gtd.Generator()
    spec = g.spec()
    n_campaigns = 100
    spec['customers']['campaigns']['count'] = n_campaigns
    spec['customers']['campaigns']['pmax']['percent'] = 100
    g.generate_model()

    actual = len(list(g.ad_group_criteria()))
    self.assertEqual(
        0, actual, 'PMax campaigns should not generate ad group criteria.'
    )

  def test_create_ad_group_criteria_shopping(self):
    g = gtd.Generator()
    spec = g.spec()
    n_campaigns = 100
    spec['customers']['campaigns']['count'] = n_campaigns
    spec['customers']['campaigns']['pmax']['percent'] = 0
    g.generate_model()

    actual = len(list(g.ad_group_criteria()))
    self.assertTrue(
        0 < actual, 'Shopping campaigns must generate some group criteria.'
    )

  def test_create_asset_group_listing_group_filter_pmax(self):
    g = gtd.Generator()
    spec = g.spec()
    n_campaigns = 100
    spec['customers']['campaigns']['count'] = n_campaigns
    spec['customers']['campaigns']['pmax']['percent'] = 100
    g.generate_model()

    actual = len(list(g.asset_group_listing_group_filters()))
    self.assertTrue(
        0 < actual,
        'PMax campaigns must generate some asset group '
        'listing group filters.',
    )

  def test_create_asset_group_listing_group_filter_shopping(self):
    g = gtd.Generator()
    spec = g.spec()
    n_campaigns = 100
    spec['customers']['campaigns']['count'] = n_campaigns
    spec['customers']['campaigns']['pmax']['percent'] = 0
    g.generate_model()

    actual = len(list(g.asset_group_listing_group_filters()))
    self.assertEqual(
        0,
        actual,
        'Shopping campaigns must not generate any asset group '
        'listing group filters.',
    )

  def test_create_language_constants(self):
    g = gtd.Generator()
    actual = len(list(g.language_constants()))
    self.assertEqual(len(gtd.LANGUAGE), actual)

  def test_create_product_categories(self):
    g = gtd.Generator()
    expected = len(gtd.PRODUCT_CATEGORIES)
    actual = len(list(g.product_categories()))
    self.assertEqual(expected, actual)

  def test_shopping_targeting_to_ad_group_criterion(self):
    customer_id = '6955'
    campaign_id = '1215537'
    campaign_type = 'SHOPPING'
    # This category is 5 levels deep. We expect a wildcard for each level
    category_id = '7552'
    expected_criteria_length = 6

    model = {
        'advertisers': [
            {
                'campaigns': [
                    {
                        'enable_local': False,
                        'feed_label': 'online',
                        'id': campaign_id,
                        'listing_scopes': [],
                        'merchant_id': 64438,
                        'targeting': [('CATEGORY', category_id)],
                        'type': 'SHOPPING',
                    }
                ],
                'id': customer_id,
            }
        ]
    }

    g = gtd.Generator(model=model)
    criteria = list(g.ad_group_criteria())
    self.assertEqual(expected_criteria_length, len(criteria))
    for i, criterion in enumerate(criteria):
      self.assertEqual(campaign_id, str(criterion.campaign.id))
      self.assertEqual(customer_id, str(criterion.customer.id))
      # All wildcards should be negative
      if i < expected_criteria_length - 1:
        self.assertTrue(criterion.ad_group_criterion.negative)
      else:
        # The last element should be positive
        self.assertFalse(criterion.ad_group_criterion.negative)
        dimension = criterion.ad_group_criterion.listing_group.path.dimensions[
            -1
        ]
        self.assertEqual(
            category_id, str(dimension.product_category.category_id)
        )


if __name__ == '__main__':
  absltest.main()
