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

"""Generate test data for ACIT.

Data generation occurs in 3 steps. First, the consumer instantiates a Generator, optionally with a
custom spec. Then, the consumer calls `generate_model()`, which creates the internal model used by
the generator for creating API resources. Finally, the consumer calls the respective methods for
each API resource.

Refer to the source for the expected format of the spec object.

The module-level `generate()` factory function uses the default settings to output settings to the
desired folder.

When run as a command line utility, the generated model is printed to stdout for debugging.
"""

from absl import app
from absl import flags

import json
import pathlib
import random
import itertools
import enum

from google.ads.googleads.v19.services.types import google_ads_service

import sys

from typing import Set, Sequence, Any, Optional, Iterable

_GENERATE_DATA_PREFIX = flags.DEFINE_string(
    'generate_data_prefix',
    '/tmp/acit',
    'The directory to which data should be output.',
)

# Merchant Center concepts

CHANNELS = [
    'local',
    'online',
]

FEED_LABELS = [
    'US',
]

# Must be lower-case.
LANGUAGE = [
    'en',
]

# A set of made up brands.
BRANDS = [
    'Hometown Market',
    'Corner Bodega',
    'Mom & Pop Shop',
    'Metro Convenience',
]

PRODUCT_TYPES = {
    'Knick Knacks': {
        'Paddy Whacks': {},
        'Tchotchkes': {
            'Doohickeys': {},
            'Whirligigs': {},
        },
    },
    'Cleaning supplies': {
        'Arma': {
            'Virumque': {
                'Cano': {
                    'Troiae': {},
                },
            },
        },
        'Olympioi': {
            'Athena': {},
            'Demeter': {},
            'Artemis': {},
        },
        'Heroes': {
            'Ajax': {},
            'Achilles': {},
            'Jason': {},
            'Dr. Clean': {},
        },
    },
}

CUSTOM_LABELS = {
    'Seasonal': {
        'Political': {
            'National': {'Pause Indefinitely': {}},
            'Local': {
                'Famous': {},
                'Infamous': {},
            },
        },
        'Annual': {
            'Spring Break': {},
            'Holiday Rush': {},
        },
    },
    'Evergreen': {
        'Best Sellers': {},
        'Forget-Me-Nots': {},
    },
    'Promotional': {
        'Viral': {},
        'Price Advantage': {},
        'Clearance': {},
    },
}


class _TargetingType(enum.IntEnum):
  CATEGORY = 1
  TYPE = 2
  CUSTOM_LABEL = 3


class _CampaignType(enum.IntEnum):
  PERFORMANCE_MAX = 1
  SHOPPING = 2


_ALL_PRODUCTS = """\
166 - Apparel & Accessories
184 - Apparel & Accessories > Costumes & Accessories
5192 - Apparel & Accessories > Costumes & Accessories > Costume Accessories
8018 - Apparel & Accessories > Costumes & Accessories > Costume Accessories > Pretend Jewelry
188 - Apparel & Accessories > Jewelry
192 - Apparel & Accessories > Jewelry > Charms & Pendants
8 - Arts & Entertainment
5710 - Arts & Entertainment > Hobbies & Creative Arts
216 - Arts & Entertainment > Hobbies & Creative Arts > Collectibles
5709 - Arts & Entertainment > Party & Celebration
96 - Arts & Entertainment > Party & Celebration > Party Supplies
5452 - Arts & Entertainment > Party & Celebration > Party Supplies > Party Favors
632 - Hardware
503739 - Hardware > Building Consumables
2277 - Hardware > Building Consumables > Chemicals
7504 - Hardware > Building Consumables > Chemicals > Concrete & Masonry Cleaners
1749 - Hardware > Building Consumables > Chemicals > Drain Cleaners
7470 - Hardware > Building Consumables > Chemicals > Septic Tank & Cesspool Treatments
536 - Home & Garden
696 - Home & Garden > Decor
5609 - Home & Garden > Decor > Figurines
5876 - Home & Garden > Decor > Refrigerator Magnets
5885 - Home & Garden > Decor > Novelty Signs
630 - Home & Garden > Household Supplies
623 - Home & Garden > Household Supplies > Household Cleaning Supplies
2610 - Home & Garden > Household Supplies > Household Cleaning Supplies > Squeegees
2796 - Home & Garden > Household Supplies > Household Cleaning Supplies > Sponges & Scouring Pads
4670 - Home & Garden > Household Supplies > Household Cleaning Supplies > Scrub Brushes
4973 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products
4976 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products > Glass & Surface Cleaners
6474 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products > Household Disinfectants
4978 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products > Oven & Grill Cleaners
7552 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products > Rinse Aids
7426 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products > Stainless Steel Cleaners & Polishes
4980 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products > Toilet Bowl Cleaners
4981 - Home & Garden > Household Supplies > Household Cleaning Supplies > Household Cleaning Products > Tub & Tile Cleaners
8071 - Home & Garden > Household Supplies > Household Cleaning Supplies > Shop Towels & General-Purpose Cleaning Cloths\
"""

_PARETO_ALPHA = 3

PRODUCT_CATEGORIES = {}
for line in _ALL_PRODUCTS.split('\n'):
  product_id, taxonomy_raw = line.split(' - ')
  taxonomy = taxonomy_raw.split(' > ')
  PRODUCT_CATEGORIES[product_id] = tuple(taxonomy)

PRODUCT_CATEGORY_IDS_BY_PATH = {
    taxonomy: product_id for product_id, taxonomy in PRODUCT_CATEGORIES.items()
}

StrDict = dict[str, 'StrDict']


def flatten_types(types: StrDict) -> list[Sequence[str]]:
  """Flatten a hierarchy of types.

  Args:
    types: The types to flatten

  Returns:
    Each individual branch of the tree.
  """
  res = []

  def f(prefix, t):
    # We want all prefixes as well
    res.append(prefix)
    for k in t:
      f(prefix + [k], t[k])

  f([], types)
  return res


def generate_pks(n, existing: Optional[set[str]] = None) -> set[str]:
  """Generate `n` new primary keys and add them to the existing set.

  This function modifies its inputs.

  Args:
    existing: The set of existing keys (may be empty). Mutated.

  Returns:
    The new keys generated.
  """

  ids: set[str] = set()
  existing = existing if existing else set()
  for _ in range(n):
    while True:
      product_id = str(int(random.random() * 10000 * n))
      if product_id not in (ids | existing):
        ids.add(product_id)
        break
  existing.update(ids)
  return ids


class Generator:
  """The Generator class. See module docstring for usage."""

  def __init__(self, spec=None, model=None) -> None:
    """Constructor.

    Args:
      spec: The spec to use for data generation. Refer to code for expected keys.
      model: The model to use. Populated through `generate_model()` if not specified.
    """

    # DONE: feed labels/countries
    # DONE: brands
    # TODO: disapprovals
    # DONE: product types  # user defined
    # DONE: product categories  # Google-defined
    # DONE: custom labels
    # DONE: merchants
    # DONE: ads accounts
    # TODO: linked ads accounts
    # DONE: pmax_campaigns
    # DONE: shopping_campaigns
    # DONE: PMax asset group listing group filters
    # Done: Shopping ad group listing group filters

    self._data = {} if not model else model
    # Statistical distributions for each attribute, used with random.choices()
    self._distributions: dict[str, list[tuple[float, Any]]] = {
        'merchants': [],
        'types': [],
        'labels': [],
        'categories': [],
        'feeds': [],
        'languages': [],
        'channels': [],
        'brands': [],
    }
    self._spec = (
        spec
        if spec
        else {
            'products': {
                'count': 1234,
            },
            'customers': {
                'count': 1,
                'campaigns': {
                    'count': 124,
                    'pmax': {
                        'percent': 44,
                    },
                    'shopping': {},
                },
            },
        }
    )

  def model(self) -> Any:
    """Returns the state of the data model. May not be populated."""
    return self._data

  def spec(self) -> Any:
    """Returns the current spec. Useful for modifying values from defaults."""
    return self._spec

  def generate_model(self) -> None:
    """Generates a brand new model, replacing the existing one."""
    merchants = self._generate_merchants()
    self._data['merchants'] = merchants
    products = self._generate_products(merchants)

    advertisers = self._generate_advertisers(merchants)
    self._data['advertisers'] = advertisers

    campaigns = self._generate_campaigns(merchants, advertisers)
    self._generate_campaign_criteria(campaigns)

  # Merchant Center objects

  def products(self) -> Any:
    """The Merchant Center products API resources for this model.

    Yields:
      Minimal Merchant Center Product API objects in Python dictionary form.
    """
    for merchant in self._data['merchants']:
      merchant_id = merchant['id']
      for product in merchant['products']:
        channel = product['channel']
        language = product['language']
        feed = product['feed']
        product_id = product['id']
        types = product['types']
        labels = product['labels']
        categories = PRODUCT_CATEGORIES[product['categories']]
        gtin = product['gtin']
        brand = product['brand']

        row = {
            'kind': 'content#product',
            'id': f'{channel}:{language}:{feed}:{product_id}',
            'offerId': product_id,
            'source': 'feed',
            'title': f'Product {product_id}',
            'description': 'Description',
            'link': 'https://google.com',
            'mobileLink': 'https://google.com',
            'imageLink': 'https://google.com',
            'contentLanguage': language,
            'targetCountry': feed,  # Generally same as country
            'feedLabel': feed,
            'channel': channel,
            'ageGroup': 'adult',
            'availability': 'in stock',  # TODO: support "out of stock"
            'brand': brand,
            'color': 'Blue',
            'condition': 'new',
            'gender': 'unisex',
            'googleProductCategory': (
                ' > '.join(categories) if categories else ''
            ),
            'mpn': '333222111',
            'price': {'value': '43.00', 'currency': 'USD'},
            'productTypes': [
                ' > '.join(types) if types else '',
            ],
            'salePrice': {'value': '32.00', 'currency': 'USD'},
            'adsRedirect': 'https://google.com',
            'pickupMethod': 'ship to store',
            'pickupSla': 'multi week',
            'customAttributes': [{
                'name': 'placeholder custom attribute name',
                'value': 'placeholder custom attribute value',
            }],
            'downloaderMetadata': {'accountId': merchant_id},
        }
        for i, label in enumerate(labels):
          row[f'customLabel{i}'] = label
        yield row

  def product_statuses(self):
    """The Merchant Center productstatus API resources for this model.

    Yields:
      Minimal Merchant Center Product Status API objects in Python dictionary form.
    """
    for merchant in self._data['merchants']:
      merchant_id = merchant['id']
      for product in merchant['products']:
        channel = product['channel']
        language = product['language']
        feed = product['feed']
        product_id = product['id']

        row = {
            'kind': 'content#productStatus',
            'productId': f'{channel}:{language}:{feed}:{product_id}',
            'title': 'Same title as before',
            'link': 'https://www.google.com',
            # TODO: Disapprovals
            'destinationStatuses': [
                {
                    'destination': 'Shopping',
                    'status': 'approved',
                    'approvedCountries': ['US'],
                },
                {
                    'destination': 'DisplayAds',
                    'status': 'approved',
                    'approvedCountries': ['US'],
                },
                {
                    'destination': 'SurfacesAcrossGoogle',
                    'status': 'approved',
                    'approvedCountries': ['US'],
                },
            ],
            'creationDate': '2020-12-20T20:08:20Z',
            'lastUpdateDate': '2020-12-20T20:08:20Z',
            'googleExpirationDate': '2022-12-20T20:08:20Z',
            'downloaderMetadata': {'accountId': merchant_id},
        }
        yield row

  # Google Ads objects

  def campaigns(self) -> Iterable[google_ads_service.GoogleAdsRow]:
    """The Google Ads campaign resources for this model.

    Yields:
      A GoogleAdsRow for each campaign.
    """
    for customer in self._data['advertisers']:
      customer_id = customer['id']
      for campaign in customer['campaigns']:
        campaign_id = campaign['id']
        merchant_id = campaign['merchant_id']
        feed_label = campaign['feed_label']
        enable_local = campaign['enable_local']
        campaign_type = campaign['type']
        d = {
            'customer': {
                'id': customer_id,
            },
            'campaign': {
                'status': 'ENABLED',
                'advertisingChannelType': campaign_type,
                'id': campaign_id,
                'shoppingSetting': {
                    'merchantId': merchant_id,
                    'feedLabel': feed_label,
                    'enableLocal': enable_local,
                },
            },
        }
        row = google_ads_service.GoogleAdsRow.from_json(json.dumps(d))
        yield row

  def campaign_criteria(self) -> Iterable[google_ads_service.GoogleAdsRow]:
    """The Google Ads campaign criteria resources for this model.

    Yields:
      A GoogleAdsRow for each campaign criterion.
    """
    assert (
        len(LANGUAGE) == 1
    ), 'please update language campaign criteria generator'
    for customer in self._data['advertisers']:
      customer_id = customer['id']
      for campaign in customer['campaigns']:
        campaign_id = campaign['id']
        # Language settings (all campaigns)
        d = {
            'customer': {
                'id': customer_id,
            },
            'campaign': {
                'status': 'ENABLED',
                'id': campaign_id,
            },
            'campaignCriterion': {
                'type': 'LANGUAGE',
                'language': {
                    # TODO: update to support additional languages if necessary
                    'languageConstant': 'languageConstants/1000',
                },
                'status': 'ENABLED',
                'negative': False,
            },
        }
        row = google_ads_service.GoogleAdsRow.from_json(json.dumps(d))
        yield row

        # Listing scopes (Shopping only)
        listing_scopes = campaign['listing_scopes']
        if (
            _CampaignType[campaign['type']] == _CampaignType.SHOPPING
            and listing_scopes
        ):
          dimensions = self._get_dimensions_from_inventory_filters(
              listing_scopes
          )
          if not dimensions:
            continue
          d = {
              'customer': {
                  'id': customer_id,
              },
              'campaign': {
                  'status': 'ENABLED',
                  'id': campaign_id,
              },
              'campaignCriterion': {
                  'type': 'LISTING_SCOPE',
                  'listingScope': {
                      # TODO: interspersed dimensions
                      'dimensions': dimensions,
                  },
                  'status': 'ENABLED',
                  'negative': False,
              },
          }
          row = google_ads_service.GoogleAdsRow.from_json(json.dumps(d))
          yield row

  def ad_group_criteria(self) -> Iterable[google_ads_service.GoogleAdsRow]:
    """The Google Ads ad group criteria resources for this model.

    Yields:
      A GoogleAdsRow for each ad group criterion.
    """
    for criterion in self._inventory_filter_criteria(_CampaignType.SHOPPING):
      customer_id = criterion['customer_id']
      campaign_id = criterion['campaign_id']
      ad_group_id = criterion['subdivision_id']
      criterion_id = criterion['criterion_id']
      path = criterion['path']
      is_inclusive = criterion['is_inclusive']

      d = {
          'customer': {
              'id': customer_id,
          },
          'campaign': {
              'id': campaign_id,
          },
          'adGroup': {
              'id': ad_group_id,
          },
          'adGroupCriterion': {
              'status': 'ENABLED',
              'type': 'LISTING_GROUP',
              'listingGroup': {
                  'type': 'UNIT',
                  'path': path,
              },
              'criterionId': criterion_id,
              'negative': not is_inclusive,
          },
      }
      row = google_ads_service.GoogleAdsRow.from_json(json.dumps(d))
      yield row

  def asset_group_listing_group_filters(
      self,
  ) -> Iterable[google_ads_service.GoogleAdsRow]:
    """The Google Ads asset group listing group filter resources for this model.

    Yields:
      A GoogleAdsRow for each asset group listing group filter.
    """
    for criterion in self._inventory_filter_criteria(
        _CampaignType.PERFORMANCE_MAX
    ):
      customer_id = criterion['customer_id']
      campaign_id = criterion['campaign_id']
      asset_group_id = criterion['subdivision_id']
      criterion_id = criterion['criterion_id']
      path = criterion['path']
      is_inclusive = criterion['is_inclusive']

      d = {
          'customer': {
              'id': customer_id,
          },
          'campaign': {
              'id': campaign_id,
          },
          'assetGroup': {
              'id': asset_group_id,
          },
          'assetGroupListingGroupFilter': {
              'id': criterion_id,
              'type': 'UNIT_EXCLUDED' if not is_inclusive else 'UNIT_INCLUDED',
              'path': path,
          },
      }

      row = google_ads_service.GoogleAdsRow.from_json(json.dumps(d))
      yield row

  def language_constants(self) -> Iterable[google_ads_service.GoogleAdsRow]:
    """The Google Ads language constant resources for this model.

    Yields:
      A GoogleAdsRow for each language constant.
    """
    assert len(LANGUAGE) == 1, 'please update language constant generator'
    d = {
        'languageConstant': {
            'resourceName': 'languageConstants/1000',
            'code': 'en',
            'name': 'English',
        }
    }
    row = google_ads_service.GoogleAdsRow.from_json(json.dumps(d))
    yield row

  def product_categories(self) -> Iterable[google_ads_service.GoogleAdsRow]:
    """The Google Ads product constant resources for this model.

    Yields:
      A GoogleAdsRow for each product constant.
    """
    assert len(LANGUAGE) == 1, 'please update language constant generator'
    for category_id, taxonomy in PRODUCT_CATEGORIES.items():
      d = {
          'productCategoryConstant': {
              'categoryId': category_id,
              'localizations': [{
                  'regionCode': 'US',
                  'languageCode': 'en',
                  # We only need the last one
                  'value': taxonomy[-1],
              }],
          }
      }
      row = google_ads_service.GoogleAdsRow.from_json(json.dumps(d))
      yield row

  def _generate_products(self, merchants):
    """Generate products, and attach them to the specified customers.

    This method modifies the input customer objects.

    Args:
      merchants: The collection of merchants to which these products should refer.

    Returns:
      The generated products, for later processing.
    """
    spec = self._spec['products']
    n_products = spec['count'] // 2  # we double later for channel

    # Precompute random distributions
    merchant_pool = self._generate_dist('merchants', merchants, n_products)
    feed_pool = self._generate_dist('feeds', FEED_LABELS, n_products)
    languages_pool = self._generate_dist('languages', LANGUAGE, n_products)
    channel_pool = self._generate_dist('channels', CHANNELS, n_products)
    id_pool = generate_pks(n_products)
    types_pool = self._generate_dist(
        'types', flatten_types(PRODUCT_TYPES), n_products
    )
    labels_pool = self._generate_dist(
        'labels', flatten_types(CUSTOM_LABELS), n_products
    )
    categories_pool = self._generate_dist(
        'categories', list(PRODUCT_CATEGORIES), n_products
    )
    gtin_pool = generate_pks(n_products)
    brand_pool = self._generate_dist('brands', BRANDS, n_products)
    products = []
    zipped = zip(
        merchant_pool,
        feed_pool,
        languages_pool,
        channel_pool,
        id_pool,
        types_pool,
        labels_pool,
        categories_pool,
        gtin_pool,
        brand_pool,
    )
    for (
        merchant,
        feed,
        languages,
        channel,
        product_id,
        types,
        labels,
        categories,
        gtin,
        brand,
    ) in zipped:
      product = {
          'feed': feed,
          'language': languages,
          'channel': channel,
          'id': product_id,
          'types': types,
          'labels': labels,
          'categories': categories,
          'gtin': gtin,
          'brand': brand,
      }
      products.append(product)
      merchant['products'].append(product)
      # For completeness, add another product in the other channel
      product = {
          'feed': feed,
          'language': languages,
          'channel': 'local' if channel == 'online' else 'online',
          'id': product_id,
          'types': types,
          'labels': labels,
          'categories': categories,
          'gtin': gtin,
          'brand': brand,
      }
      products.append(product)
      merchant['products'].append(product)
    return products

  def _generate_merchants(self):
    # for simplicity's sake, we'll re-use the brands under the assumption that they all sell each others' products.
    return [
        {'name': brand, 'id': str(random.randint(10000, 99999)), 'products': []}
        for brand in BRANDS
    ]

  def _generate_advertisers(self, merchants):
    # TODO: refine
    customers_spec = self._spec['customers']
    n_customers = customers_spec['count']

    customer_ids = generate_pks(n_customers)

    customers = [
        {'id': customer_id, 'campaigns': []} for customer_id in customer_ids
    ]
    return customers

  def _targeting_choices(self, n_campaigns: int):
    # TODO: Use more than just one targeting type per campaign
    targeting_types = {
        _TargetingType.CATEGORY: self._distributions['categories'],
        _TargetingType.TYPE: self._distributions['types'],
        _TargetingType.CUSTOM_LABEL: self._distributions['labels'],
    }

    # Each product's targeting follows a distribution. Use that.
    targeting_options = []
    targeting_weights = []
    for k, v in targeting_types.items():
      for e in v:
        weight, option = e
        targeting_options.append((k.name, option))
        targeting_weights.append(weight)

    targeting_choices = random.choices(
        population=targeting_options,
        # May need to interweave targeting options.
        weights=targeting_weights,
        k=n_campaigns,
    )
    return targeting_choices

  def _generate_campaigns(self, merchants, customers):
    """Generate campaigns, and attach them to the specified customers.

    This method modifies the input customer objects.

    Args:
      merchants: The collection of merchants to which these campaigns should refer.
      customers: The customers to which these campaigns should belong.

    Returns:
      The generated campaigns, for later processing.
    """
    customers_spec = self._spec['customers']
    campaigns_spec = customers_spec['campaigns']
    n_campaigns = campaigns_spec['count']
    campaign_ids = generate_pks(n_campaigns)
    is_pmax_pct = campaigns_spec['pmax']['percent'] / 100
    campaign_types = random.choices(
        population=[_CampaignType.PERFORMANCE_MAX, _CampaignType.SHOPPING],
        weights=[is_pmax_pct, 1 - is_pmax_pct],
        k=n_campaigns,
    )
    merchant_assignments = random.choices(
        population=merchants,
        weights=[
            random.paretovariate(_PARETO_ALPHA) for _ in range(len(merchants))
        ],
        k=n_campaigns,
    )
    channel_assignments = random.choices(
        population=CHANNELS,
        # Unnecessary, but keeping as a placeholder in case we want to adjust with config
        weights=itertools.repeat(0.5, len(CHANNELS)),
        k=n_campaigns,
    )

    customer_assignments = random.choices(
        population=customers,
        k=n_campaigns,
    )
    feed_assignments = random.choices(
        population=FEED_LABELS,
        k=n_campaigns,
    )

    targeting_choices = self._targeting_choices(n_campaigns)

    campaigns = []

    for (
        campaign_id,
        customer,
        campaign_type,
        merchant_assignment,
        channel_assignment,
        feed_assignment,
        targeting_choice,
    ) in zip(
        campaign_ids,
        customer_assignments,
        campaign_types,
        merchant_assignments,
        channel_assignments,
        feed_assignments,
        targeting_choices,
    ):
      campaign = {
          'id': campaign_id,
          'merchant_id': merchant_assignment['id'],
          'type': campaign_type.name,
          'feed_label': feed_assignment,  # TODO: Can this be empty?
          'enable_local': channel_assignment == 'local',
          # TODO: support negative targeting
          #
          # Shopping campaigns may have listing scopes, always positive, narrowing product scope.
          # For simplicity, for shopping campaigns, we'll randomly re-use various parts of the
          # targeting choice, and keep everything positive. Of course, real usage isn't always like
          # this, so we may want to modify later.
          'targeting': [targeting_choice],
          'listing_scopes': [],
      }
      customer['campaigns'].append(campaign)
      campaigns.append(campaign)
    return campaigns

  def _generate_campaign_criteria(self, campaigns):
    """Generates campaign criteria.

    This is primarily used for shopping campaign listing scopes. Modifies campaigns in-place.

    Args:
      campaigns: The list of campaigns to generating criteria for.
    """

    # Selectively add listing scopes.
    # only for certain shopping campaigns
    # only positive
    # TODO: modify this to deal with negative targeting
    # pop off the campaign targeting
    # TODO: limit to a particular level
    campaigns_and_scope = zip(
        campaigns, random.choices(population=[True, False], k=len(campaigns))
    )
    for campaign, is_scoped in campaigns_and_scope:
      if (
          _CampaignType[campaign['type']] == _CampaignType.SHOPPING
          and is_scoped
      ):
        campaign['listing_scopes'].append(campaign['targeting'].pop())

  def _inventory_filter_criteria(self, campaign_type: _CampaignType) -> Any:
    """Common logic for generating inventory filter criteria for Shopping and PMax campaigns.

    Args:
      campaign_type: The campaign type to generate.

    Returns:
      The spec for the inventory filter.
    """
    ad_group_ids: set[str] = set()
    ad_group_criterion_ids: set[str] = set()
    for customer in self._data['advertisers']:
      customer_id = customer['id']
      for campaign in customer['campaigns']:
        inventory_filters = campaign['targeting']
        if _CampaignType[campaign['type']] == campaign_type:
          campaign_id = campaign['id']
          ad_group_id = list(generate_pks(1, ad_group_ids))[0]
          # TODO: support negative targeting

          # TODO: support filter tree (not just one path)

          # If `inventory_filters` is empty, we return only one listing group criterion.
          # In all other cases, a wildcard must explicitly exist to catch other paths.
          inventory_filter_dimensions = (
              self._get_dimensions_from_inventory_filters(inventory_filters)
          )
          inclusivity_and_paths: list[tuple[bool, dict]] = []
          if inventory_filter_dimensions:
            # The root cannot contain wildcards
            wildcard_root: list[Any] = []
            for dimension in inventory_filter_dimensions:
              wildcard: Any = {k: {} for k in list(dimension)}
              inclusivity_and_paths.append(
                  (False, {'dimensions': wildcard_root + [wildcard]})
              )
              wildcard_root.append(dimension)
            inclusivity_and_paths.append(
                (True, {'dimensions': inventory_filter_dimensions})
            )
          else:
            # No dimensions at all, target everything with a wildcard
            inclusivity_and_paths.append((True, {}))

          criterion_ids = generate_pks(
              len(inclusivity_and_paths), ad_group_criterion_ids
          )
          for criterion_id, inclusivity_and_path in zip(
              criterion_ids, inclusivity_and_paths
          ):
            is_inclusive, path = inclusivity_and_path
            yield {
                'customer_id': customer_id,
                'campaign_id': campaign_id,
                # TODO: rename to be generic
                'subdivision_id': ad_group_id,
                'criterion_id': criterion_id,
                'path': path,
                'is_inclusive': is_inclusive,
            }

  def _get_dimensions_from_inventory_filters(self, inventory_filters):
    dimensions = []
    # TODO: support interleaved listing scopes, brand, product ID, etc
    if not inventory_filters:
      return dimensions
    inventory_filter_type, path = inventory_filters[0]
    if _TargetingType[inventory_filter_type] == _TargetingType.CATEGORY:
      # given a product category ID, return all IDs for elements above it
      taxonomy = PRODUCT_CATEGORIES[path]
      curr = []
      # Levels are 1-indexed, for some reason
      for i, segment in enumerate(taxonomy, start=1):
        curr.append(segment)
        dimension = {
            'productCategory': {
                'level': f'LEVEL{i}',
                'categoryId': PRODUCT_CATEGORY_IDS_BY_PATH[tuple(curr)],
            }
        }
        dimensions.append(dimension)
    elif _TargetingType[inventory_filter_type] == _TargetingType.TYPE:
      for i, segment in enumerate(path, start=1):
        dimension = {
            'productType': {
                'level': f'LEVEL{i}',
                # must match case-insensitively across systems
                'value': segment.upper(),
            }
        }
        dimensions.append(dimension)
    elif _TargetingType[inventory_filter_type] == _TargetingType.CUSTOM_LABEL:
      for i, segment in enumerate(path):
        dimension = {
            'productCustomAttribute': {
                'index': f'INDEX{i}',
                'value': segment.upper(),
            }
        }
        dimensions.append(dimension)
    return dimensions

  def _generate_dist(self, key: str, values: list, over: int):
    """Generate a Pareto distribution for each element in this collections.

    Stores a tuple of distribution and value on the generator object, for later reference.

    Args:
      key: The key to store this distribution as. Useful for re-using later.
      values: The values to assign a distribution for.
      over: The total number of choices to take.

    Returns:
      The choices for this distribution.
    """
    if not self._distributions[key]:
      weights = [
          random.paretovariate(_PARETO_ALPHA) for _ in range(len(values))
      ]
      self._distributions[key] = list(zip(weights, values))
    return random.choices(population=values, weights=weights, k=over)


def generate(output_dir: str) -> Any:
  """Runs the full data generation pipeline.

  Args:
    output_dir: The directory to output the data.

  Returns:
    The raw object model, for debugging.
  """

  timestamp = '2020-01-01Tsomething'
  account_id = '123'

  generator = Generator()
  generator.generate_model()

  resource_names_and_generators_by_system = {
      'ads': (
          ('campaign', generator.campaigns),
          ('campaign_criterion', generator.campaign_criteria),
          ('ad_group_criterion', generator.ad_group_criteria),
          (
              'asset_group_listing_filter',
              generator.asset_group_listing_group_filters,
          ),
          ('product_category', generator.product_categories),
          ('language_constant', generator.language_constants),
      ),
      'merchant_center': (
          ('products', generator.products),
          ('productstatuses', generator.product_statuses),
      ),
  }
  for system in resource_names_and_generators_by_system:
    if system == 'ads':
      converter = lambda row: type(row).to_json(
          row,
          use_integers_for_enums=False,
          including_default_value_fields=False,
          indent=None,
      )
    else:  # system == 'merchant_center'
      converter = lambda row: json.dumps(row)
    for resource_name, callback in resource_names_and_generators_by_system[
        system
    ]:
      path = f'{output_dir}/{timestamp}/{system}/{account_id}/{resource_name}/rows.jsonlines'
      pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
      with open(path, 'wt') as f:
        for row in callback():
          print(converter(row), file=f)
  return generator.model()


def main(argv: Sequence[str]):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')
  import pprint

  output_dir = _GENERATE_DATA_PREFIX.value
  model = generate(output_dir)
  pprint.pprint(model)


if __name__ == '__main__':
  app.run(main)
